var log = require('azure-mobile-apps').logger;

module.exports = {
    createDocument: function (client, collectionRef, docObject, callback) {
        log.info(`[documentdb/ops/document] createDocument(${collectionRef.id}, ${JSON.stringify(docObject)})`);
        client.createDocument(collectionRef._self, docObject, callback);
    },

    deleteDocument: function (client, docLink, callback) {
        client.deleteDocument(docLink, callback);
    },

    fetchDocument: function (client, collectionRef, docId, callback) {
        log.info(`[documentdb/ops/document] fetchDocuemnt(${collectionRef.id}, ${docId})`);
        let querySpec = {
            query: 'SELECT * FROM root r WHERE r.id=@id',
            parameters: [
                { name: '@id', value: docId }
            ]
        };
        client.queryDocuments(collectionRef._self, querySpec).current(callback);
    },

    queryDocuments: function(client, collectionRef, query, callback) {
        client.queryDocuments(collectionRef._self, query).toArray(callback);
    },

    readDocument: function(client, docLink, options, callback) {
        client.readDocument(docLink, options, callback);
    },

    readDocuments: function(client, collectionRef, callback) {
        client.readDocuments(collectionRef._self).toArray(callback);
    },

    replaceDocument: function(client, docLink, docObject, callback) {
        client.replaceDocument(docLink, docObject, callback);
    }
};
