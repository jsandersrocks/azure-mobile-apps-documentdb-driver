module.exports = {
    createDatabase: function (client, databaseId, callback) {
        client.createDatabase({ id: databaseId }, callback);
    },

    deleteDatabase: function (client, databaseId, callback) {
        client.deleteDatabase(`dbs/${databaseId}`, callback);
    },

    findDatabaseById: function (client, databaseId, callback) {
        var querySpec = {
            query: 'SELECT * FROM root r WHERE r.id = @id',
            parameters: [ { name: '@id', value: databaseId } ]
        };

        client.queryDatabases(querySpec).toArray(function (err, results) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, (results.length === 0) ? null : results[0]);
            }
        });
    },

    listDatabases: function (client, callback) {
        client.readDatabases().toArray(callback);
    },

    readDatabase: function (client, database, callback) {
        client.readDatabase(database._self, callback);
    },

    readDatabases: function (client, databaseId, callback) {
        client.readDatabase(`dbs/${databaseId}`, callback);
    }
};
