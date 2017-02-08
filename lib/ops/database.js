const log = require('azure-mobile-apps').logger;

module.exports = {
    createDatabase: function (client, databaseId, callback) {
        log.silly(`[documentdb/ops/database] createDatabase ${databaseId}`);
        client.createDatabase({ id: databaseId }, callback);
    },

    deleteDatabase: function (client, databaseId, callback) {
        log.silly(`[documentdb/ops/database] deleteDatabase ${databaseId}`);
        client.deleteDatabase(`dbs/${databaseId}`, callback);
    },

    findDatabaseById: function (client, databaseId, callback) {
        log.silly(`[documentdb/ops/database] findDatabaseById ${databaseId}`);
        var qs = {
            query: 'SELECT * FROM root r WHERE r.id = @id',
            parameters: [
                { name: '@id', value: databaseId }
            ]
        };

        client.queryDatabases(qs).toArray(function (err, results) {
            if (err) {
                log.error(`[documentdb/ops/database] findDatabaseById error = ${JSON.stringify(err)}`);
                callback(err, null);
            } else {
                log.silly(`[documentdb/ops/database] findDatabaseById results = ${JSON.stringify(results)}`);
                callback(null, (results.length === 0) ? null : results[0]);
            }
        });
    },

    listDatabases: function (client, callback) {
        log.silly(`[documentdb/ops/database] listDatabases`);
        client.readDatabases().toArray(callback);
    },

    readDatabase: function (client, database, callback) {
        log.silly(`[documentdb/ops/database] readDatabase ${database._self}`);
        client.readDatabase(database._self, callback);
    },

    readDatabases: function (client, databaseId, callback) {
        log.silly(`[documentdb/ops/database] readDatabases ${databaseId}`);
        client.readDatabase(`dbs/${databaseId}`, callback);
    }
};
