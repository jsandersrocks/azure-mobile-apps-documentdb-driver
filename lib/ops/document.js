module.exports = {
    createDocument: function (client, collectionRef, docObject, callback) {
        client.createDocument(collectionRef._self, docObject, callback);
    },

    deleteDocument: function (client, docLink, callback) {
        client.deleteDocument(docLink, callback);
    },

    fetchDocument: function (client, collectionRef, docId, callback) {
        let querySpec = {
            query: 'SELECT * FROM root r WHERE r.id=@id',
            parameters: [
                { name: '@id', value: docId }
            ]
        };
        client.queryDocuments(collectionRef._self, querySpec).current(callback);
    },

    queryDocuments: function (client, collectionRef, query, callback) {
        client.queryDocuments(collectionRef._self, query).toArray(callback);
    },

    readDocument: function (client, docLink, options, callback) {
        client.readDocument(docLink, options, callback);
    },

    readDocuments: function (client, collectionRef, callback) {
        client.readDocuments(collectionRef._self).toArray(callback);
    },

    replaceDocument: function (client, docLink, docObject, callback) {
        client.replaceDocument(docLink, docObject, callback);
    }
};
