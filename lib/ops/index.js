const zumo = require('azure-mobile-apps');
const database = require('./database');
const collection = require('./collection');
const docOps = require('./document');
const log = zumo.logger;
const promises = zumo.promises;

let dbCache = {};

const createDatabase = promises.wrap(database.createDatabase);
const findDatabaseById = promises.wrap(database.findDatabaseById);

function ensureDatabaseExists (client, database) {
    log.verbose(`[documentdb/ops] ensureDatabaseExists(${database})`);
    if (database in dbCache) {
        log.verbose(`[documentdb/ops/ensureDatabaseExists] database link exists in cache`);
        return promises.resolve(dbCache[database]);
    }

    return findDatabaseById(client, database)
    .then((dbRef) => {
        log.verbose(`[documentdb/ops/ensureDatabaseExists] dbRef = ${JSON.stringify(dbRef, null, 2)}`);
        if (dbRef == null) {
            log.verbose(`[documentdb/ops/ensureDatabaseExists] need to create database`);
            return createDatabase(client, database).then((dbRef) => {
                dbCache[database] = dbRef;
                return dbRef;
            });
        }
        dbCache[database] = dbRef;
        return dbRef;
    })
    .catch((err) => {
        log.error(`[documentdb/ops/ensureDatabaseExists] error = `, err);
    });
}

module.exports = {
    createCollection: promises.wrap(collection.createCollection),
    listCollections: promises.wrap(collection.listCollections),
    readCollection: promises.wrap(collection.readCollection),
    readCollectionById: promises.wrap(collection.readCollectionById),
    getOfferType: promises.wrap(collection.getOfferType),
    changeOfferType: promises.wrap(collection.changeOfferType),
    deleteCollection: promises.wrap(collection.deleteCollection),

    createDatabase: createDatabase,
    deleteDatabase: promises.wrap(database.deleteDatabase),
    ensureDatabaseExists: ensureDatabaseExists,
    findDatabaseById: findDatabaseById,
    listDatabases: promises.wrap(database.listDatabases),
    readDatabase: promises.wrap(database.readDatabase),
    readDatabases: promises.wrap(database.readDatabases),

    createDocument: promises.wrap(docOps.createDocument),
    deleteDocument: promises.wrap(docOps.deleteDocument),
    fetchDocument: promises.wrap(docOps.fetchDocument),
    queryDocuments: promises.wrap(docOps.queryDocuments),
    readDocument: promises.wrap(docOps.readDocument),
    readDocuments: promises.wrap(docOps.readDocuments),
    replaceDocument: promises.wrap(docOps.replaceDocument)
};
