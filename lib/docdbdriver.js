const moment = require('moment');
const zumo = require('azure-mobile-apps');
const DocumentDb = require('documentdb');
const ops = require('./ops');
const formatSql = require('./odata-sql').format;

const log = zumo.logger;
const queries = zumo.query;

let driverSettings = null;
let dbClient = null;
let refs = {
    database: null,
    tables: {}
};

// Defaults for the driver settings
const defaults = {
    database: 'AzureMobile',
    connectionPolicy: undefined,
    consistencyLevel: 'Session',
    pricingTier: 'S1'
};

if (process.env.hasOwnProperty('CUSTOMCONNSTR_MS_DocumentDbConnectionString')) {
    let connStr = process.env['CUSTOMCONNSTR_MS_DocumentDbConnectionString'];
    let re = new RegExp(/^AccountEndpoint=([^;]+);AccountKey=([^;]+);/);
    let results = re.exec(connStr);
    if (results !== null) {
        defaults.host = results[1];
        defaults.accountKey = results[2];
    }
}
/**
 * Configure the driver for use.
 * @param {dataConfiguration} configuration the configuration of the driver.
 * @return {void}
 */
function configureDriver (configuration) {
    log.silly(`[docdbdriver] configureDriver.  Configuration = ${JSON.stringify(configuration, null, 2)}`);
    driverSettings = {
        host: configuration.host || '',
        accountKey: configuration.accountKey || '',
        database: configuration.database || defaults.database,
        connectionPolicy: configuration.connectionPolicy || defaults.connectionPolicy,
        consistencyLevel: configuration.consistencyLevel || defaults.consistencyLevel,
        pricingTier: configuration.pricingTier || defaults.pricingTier
    };
    log.silly(`[docdbdriver] driver config = ${JSON.stringify(driverSettings, null, 2)}`);
    dbClient = new DocumentDb.DocumentClient(
        driverSettings.host,
        { masterKey: driverSettings.accountKey },
        driverSettings.connectionPolicy,
        driverSettings.consistencyLevel);
    log.silly(`[documentdb-driver] dbClient = `, dbClient);
}

/**
 * Initialize the table so that it is "warmed up" and ready for use.
 * 1) Create appropriate schema if necessary
 * 2) Insert items intot he table specified by the seed property of the table configuration
 * 3) Perform any other table initialization, such as index creation
 *
 * @param {tableConfiguration} table the table configuration
 * @return {Promise}
 * @see https://github.com/Azure/azure-mobile-apps-node/blob/master/src/data/contributor.md#initialize
 */
function initializeTable (table) {
    log.silly(`[docdbdriver] initializing table.  config = ${JSON.stringify(table, null, 2)}`);

    return ops.ensureDatabaseExists(dbClient, driverSettings.database)
        .then((databaseRef) => {
            log.silly(`[docdbdriver] Initialized database ${driverSettings.database}.  Reference = ${JSON.stringify(databaseRef, null, 2)}`);
            refs.database = databaseRef;

            return ops.listCollections(dbClient, refs.database).then((collections) => {
                const collection = collections.find((c) => { return (c.id === table.name); });
                if (collection !== 'undefined') {
                    return collection;
                }
                log.info(`[docdbdriver] Creating Collection ${table.name} in database ${driverSettings.database}`);
                return ops.createCollection(dbClient, table.pricingTier || driverSettings.pricingTier, refs.database, table.name).then((cref) => {
                    // XXX-TODO: Seed Data
                    return cref;
                });
            });
        })
        .then((cref) => {
            log.silly(`[docdbdriver] Initialized collection: reference = ${JSON.stringify(cref, null, 2)}`);
            refs.tables[table.name] = cref;
        });
}

/**
 * Resolve to an object that contains the structure of the table.  Since this
 * is NoSQL, we don't have a schema, so we return something vaguely appropriate.
 * @param {Table} table the table definition
 * @return {Promise.<object>} the schema object
 * @see https://github.com/Azure/azure-mobile-apps-node/blob/master/src/data/contributor.md#schema
 */
function getSchema (table) {
    log.silly(`[docdbdriver] getSchema for tableConfig = ${JSON.stringify(table, null, 2)}`);
    return promises.resolve({
        name: table.name,
        properties: []
    });
}

/**
 * Clears all the items from the table, resolving a promise when complete
 * @param {Table} table the table definition
 * @return {Promise}
 * @see https://github.com/Azure/azure-mobile-apps-node/blob/master/src/data/contributor.md#truncate
 */
function truncateTable (table) {
    log.silly(`[docdbdriver] truncateTable for tableConfig = ${JSON.stringify(table, null, 2)}`);
    return ops.readDocuments(dbClient, refs.tables[table.name])
    .then((documents) => {
        let plist = [];
        documents.forEach((v) => {
            plist.push(ops.deleteDocument(dbClient, v._self));
        });
        return promises.all(plist);
    });
}

/**
 * Reads documents from the store.  The query parameter is from azure-query-js.  It exposes a LINQ
 * style API and exposes a comprehensive expression tree.  Support for conversion to other formats
 * is limited and generally needs to be written separately.
 *
 * The read function should resolve to an array of results.  When the provided query has the includeTotalCount
 * property set, the array should have an additional propery count that would be returned without a result size
 * limit.
 *
 * When the provided query has the includeDeleted property set, the results should include soft deleted items.
 *
 * All version values should be Base64 encoded.
 *
 * @param {Table} table the table definition
 * @param {Query} query an azure-query-js Query object
 * @return {Promise.<object>} results
 * @see https://github.com/Azure/azure-mobile-apps-node/blob/master/src/data/contributor.md#read
 */
function readOperation (table, query) {
    log.silly(`[docdbdriver/readOperation] table=${table.name}: `);

    let sqlCommands = convertQueryToSql(table, query);
    return ops.queryDocuments(dbClient, refs.tables[table.name], sqlCommands[0])
    .then((documents) => {
        // Clean up each record that is being returned
        documents.forEach((v, i) => { documents[i] = convertItemForTransmission(v); });

        // XXX-TODO: DocumentDb does not support SKIP/TAKE - fix when DocumentDb supports it
        if (sqlCommands.length == 2) {
            return {
                results: documents,
                count: documents.length
            };
        } else {
            return documents;
        }
    });
}

/**
 * The insert operation should insert a new record into the database and resolve to the inserted item.
 *
 * If an item with the same id property already exists, an Error should be thrown with the duplicate property set.
 * The createdAt and updatedAt properties should be set to the current date/time
 * The version property should be set to a unique values
 *
 * @param {Table} table the table definition
 * @param {object} item the item to be inserted
 * @return {Promise.<object>} resolve to the inserted item
 * @see https://github.com/Azure/azure-mobile-apps-node/blob/master/src/data/contributor.md#insert
 */
function insertOperation (table, item) {
    log.silly(`[docdbdriver] insertOperation table=${table.name}: ${JSON.stringify(item, null, 2)}`);
    item.createdAt = moment().toISOString();
    delete item.updatedAt;  // This is _ts and is updated for us
    delete item.version;    // This is _etag and is updated for us
    if (!item.hasOwnProperty('deleted')) item.deleted = false;

    return ops.createDocument(dbClient, refs.tables[table.name], item)
    .then((doc) => {
        log.silly(`[docdbdriver/insertOperation] inserted item ${JSON.stringify(doc)}`);
        return convertItemForTransmission(doc);
    })
    .catch((error) => {
        log.error(`[docdbdriver/insertOperation] error inserting item: `, error);
        let rejectedError = new Error();
        rejectedError.message = error.message;
        if (error.code === 409) rejectedError.duplicate = true;
        throw rejectedError;
    });
}

/**
 * The update operation should update the item with the corresponding id in the database and resolve to the
 * updated item.
 *
 * If the version property is specified, it should only update the record if the version property matches
 * If the query parameter is specified, it should only update the record if the query returns the record being updated.
 * If the version property does not match or the query does not return the record, an Error should be thrown with the concurrency property set to true
 * The updatedAt and version properties should be updated
 *
 * The query parameter is optional and allows filters such as user IDs to be applied to update operations.  The query is in
 * the format described in the read section.
 *
 * @param {Table} table the table definition
 * @param {Query} query an azure-query-js Query object
 * @return {Promise.<object>} the updated object
 * @see https://github.com/Azure/azure-mobile-apps-node/blob/master/src/data/contributor.md#update
 */
function updateOperation (table, item, query) {
    log.silly(`[docdbdriver] updateOperation table=${table.name}: ${JSON.stringify(item, null, 2)}`);

    let readPromise;
    if (typeof query == 'undefined') {
        log.silly(`[docbdriver/updateOperation]: doing read of explicit ID`);
        let querySpec = {
            query: 'SELECT * FROM root r WHERE r.id=@id',
            parameters: [
                { name: '@id', value: item.id }
            ]
        };
        readPromise = ops.queryDocuments(dbClient, refs.tables[table.name], querySpec);
    } else {
        log.silly(`[docbdriver/updateOperation]: doing read of explicit ID`);
        let sqlCommands = convertQueryToSql(table, query);
        readPromise = ops.queryDocuments(dbClient, refs.tables[table.name], sqlCommands[0]);
    }

    return readPromise.then((documents) => {
        let replacedDocPromise = null;
        documents.forEach((v) => {
            if (v.id === item.id) {
                if (item.hasOwnProperty('version')) {
                    let etag = new Buffer(item.version, 'base64').toString('ascii');
                    if (etag === v._etag) {
                        delete item.updatedAt;
                        delete item.version;
                        item.createdAt = v.createdAt;
                        item.deleted = v.deleted;
                        replacedDocPromise = ops.replaceDocument(dbClient, v._self, item);
                    }
                }
            }
        });
        if (replacedDocPromise !== null) {
            return replacedDocPromise;
        } else {
            let err = new Error("No matching records");
            err.concurrency = true;
            throw err;
        }
    }).then((doc) => { return convertItemForTransmission(doc); });
}

/**
 * The delete operation should delete any records matching the provided query.
 *
 * If a single item is deleted, it should resolve to the deleted item.  This is the
 * behavior that is exposed to the client.
 * If multiple items are deleted, it should resolve to an array of those items.
 * If the version parameter is specified, it should only delete records that match the version.
 * If no records are deleted, either because the version property does not match or the query returns
 * no records, an Error should be thrown with the concurrency property set to true.
 * If the softDelete option is specified on the table configuration, the record should be recoverable
 * by calling undelete and should be queryable by specifying the includeDeleted option on read queries.
 *
 * The query object is in the format described in the read section.  For simple data provider scenarios,
 * the query object has an id property corresponding with the value passed in the querystring of delete
 * requests
 *
 * @param {Table} table the table definition
 * @param {Query} query an azure-query-js Query object
 * @param {string} version the version of the record to be deleted
 * @return {Array|object} the deleted records
 * @see https://github.com/Azure/azure-mobile-apps-node/blob/master/src/data/contributor.md#delete
 */
function deleteOperation (table, query, version) {
    log.silly(`[docdbdriver] deleteOperation table=${table.name}: ${JSON.stringify(item, null, 2)}`);
    let sqlCommands = convertQueryToSql(table, query);
    return ops.queryDocuments(dbClient, refs.tables[table.name], sqlCommands[0])
    .then((documents) => {
        let deletedRecords = [];
        let updatePromises = [];

        documents.forEach((v) => {
            if (typeof version !== 'undefined') {
                let etag = new Buffer(version, 'base64').toString('ascii');
                if (etag !== v._etag) {
                    let err = new Error("Version Concurrency error");
                    err.concurrency = true;
                    throw err;
                }
            }
            if (table.softDelete == true) {
                v.deleted = true;
                updatePromises.push(ops.replaceDocument(dbClient, v._self, v));
            } else {
                updatePomises.push(ops.deleteDocument(dbClient, v._self));
            }
            deletedRecords.push(convertItemForTransmission(v));
        });

        return promises.all(updatePromises).then(() => { return deletedRecords; });
    })
    .then((documents) => {
        return (documents.length == 1) ? documents[0] : documents;
    });
}

/**
 * The undelete operation should restore any records matching the provided query.
 *
 * If a single item is restored, it should resolve to the restored item.  This is the
 * behavior that is exposed to the client.
 * If multiple items are restored, it should resolve to an array of those items.
 * If the version parameter is specified, it should only restore records that match the version.
 * If no records are restored, either because the version property does not match or the query returns
 * no records, an Error should be thrown with the concurrency property set to true.
 * If the softDelete option is not specied on the table configured, it shoudl resolve to undefined.
 *
 * The query object is in the format described in the read section.  For simple data provider scenarios,
 * the query object has an id property corresponding with the value passed in the querystring of delete
 * requests
 *
 * @param {Table} table the table definition
 * @param {Query} query an azure-query-js Query object
 * @param {string} version the version of the record to be deleted
 * @return {Array|object} the deleted records
 * @see https://github.com/Azure/azure-mobile-apps-node/blob/master/src/data/contributor.md#undelete
 */
function undeleteOperation (table, query, version) {
    log.silly(`[docdbdriver] undeleteOperation table=${table.name}: ${JSON.stringify(item, null, 2)}`);
    let sqlCommands = convertQueryToSql(table, query);
    return ops.queryDocuments(dbClient, refs.tables[table.name], sqlCommands[0])

    return ops.queryDocuments(dbClient, refs.tables[table.name], sqlCommands[0])
    .then((documents) => {
        let restoredRecords = [];
        let updatePromises = [];

        documents.forEach((v) => {
            if (typeof version !== 'undefined') {
                let etag = new Buffer(version, 'base64').toString('ascii');
                if (etag !== v._etag) {
                    let err = new Error("Version Concurrency error");
                    err.concurrency = true;
                    throw err;
                }
            }
            v.deleted = false;
            updatePromises.push(ops.replaceDocument(dbClient, v._self, v));
            restoredRecords.push(convertItemForTransmission(v));
        });

        return promises.all(updatePromises).then(() => { return restoredRecords; });
    })
    .then((documents) => {
        return (documents.length == 1) ? documents[0] : documents;
    });
}

/**
 * DocumentDb does not store all the fields for the objects going to the client in the database.
 * Explicitly, _ts is updatedAt and _etag is version (which must be Base64).  We need to convert
 * the object so it can be sent across the wire.
 *
 * @param {object} v the object to transmit
 * @return {object} the converted item
 * @private
 */
function convertItemForTransmission(v) {
    if (v.hasOwnProperty('_etag')) {
        v.version = new Buffer(v._etag).toString('base64');
        delete v._etag;
    }

    if (v.hasOwnProperty('_ts')) {
        v.updatedAt = moment.unix(v._ts).toISOString();
        delete v._ts;
    }

    if (v.hasOwnProperty('_rid')) delete v._rid;
    if (v.hasOwnProperty('_self')) delete v._self;
    if (v.hasOwnProperty('_attachments')) delete v._attachments;
    return v;
}

/**
 * The inbound filter uses updatedAt to do incremental sync.  We need it to be _ts.
 * In addition, we need to convert dates.
 *
 * @param {string} filter the filter to convert
 * @return {string} the converted filter
 * @private
 */
function adjustFilter(v) {
    while (/updatedAt [a-z]+ '[^']+'/.test(v)) {
        let re = new RegExp(/updatedAt ([a-z]+) '([^']+)'/i);
        let results = re.exec(v);
        v = v.replace(results[0], `_ts ${results[1]} ${moment(results[2]).unix()}`);
    }
    return v;
}

/**
 * Convert an azure-query-js Query to the DocumentDb SQL sqlCommands
 *
 * @param {Table} table the table definition
 * @param {Query} query the azure-query-js query object
 * @return {Array} sqlCommands an array of SQL sqlCommands
 * @private
 */
function convertQueryToSql(table, query) {
    let odataQuery = zumo.query.toOData(query);
    log.silly(`[docdbdriver/convertQueryToSql] odataQuery (before)=${JSON.stringify(odataQuery, null, 2)}`);
    // XXX-TODO: DocumentDb does not support SKIP/TAKE - add when DocumentDb supports it
    odataQuery.take = undefined;
    odataQuery.skip = undefined;
    // Fix for updatedAt to _ts translation in the filter
    if (odataQuery.filters) {
        if (Array.isArray(odataQuery.filters)) {
            odataQuery.filters.forEach((v, i) => { odataQuery.filters[i] = adjustFilter(v); });
        } else {
            odataQuery.filters = adjustFilter(odataQuery.filters);
        }
    }
    // Fix for updatedAt to _ts translation in the $select clause
    if (odataQuery.selections) {
        odataQuery.selections = odataQuery.selections.replace(/,{0,1}updatedAt/g, '');
    }
    // XXX-TODO: Deal with orderBy updatedAt as well
    log.silly(`[docdbdriver/convertQueryToSql] odataQuery (after)=${JSON.stringify(odataQuery, null, 2)}`);

    let sqlCommands = formatSql(odataQuery, table);
    log.silly(`[docdbdriver/convertQueryToSql] sqlCommands=${JSON.stringify(sqlCommands, null, 2)}`);

    return sqlCommands;
}

module.exports = {
    configure: configureDriver,
    initialize: initializeTable,
    schema: getSchema,
    truncate: truncateTable,

    read: readOperation,
    insert: insertOperation,
    update: updateOperation,
    delete: deleteOperation,
    undelete: undeleteOperation
};
