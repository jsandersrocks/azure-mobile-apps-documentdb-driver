var log = require('azure-mobile-apps').logger;

module.exports = {
    createCollection: function (client, pricingTier, databaseRef, collectionId, callback) {
        var options = { offerType: pricingTier };

        log.silly(`[documentdb/ops/collection] createCollection(${databaseRef.id}, ${collectionId})`);
        client.createCollection(databaseRef._self, { id: collectionId }, options, callback);
    },

    listCollections: function (client, databaseRef, callback) {
        log.silly(`[documentdb/ops/collection] listCollections(${databaseRef.id})`);
        client.readCollections(databaseRef._self).toArray(callback);
    },

    readCollection: function (client, selfLink, callback) {
        log.silly(`[documentdb/ops/collection] readCollection(${selfLink})`);
        client.readCollection(selfLink, callback);
    },

    readCollectionById: function (client, databaseRef, collectionId, callback) {
        log.silly(`[documentdb/ops/collection] readCollectionById(${databaseRef.id}, ${collectionId})`);
        const link = `${databaseRef._self}${databaseRef._colls}${collectionId}`;
        client.readCollection(link, callback);
    },

    getOfferType: function (client, collection, callback) {
        log.silly(`[documentdb/ops/collection] getOfferType(${collection._self})`);
        const querySpec = {
            query: 'SELECT * FROM root r WHERE r.resource = @link',
            parameters: [
                { name: '@link', value: collection._self }
            ]
        };
        client.queryOffers(querySpec).toArray(function (err, offers) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, (offers.length === 0) ? null : offers[0]);
            }
        });
    },

    changeOfferType: function (client, offerId, updatedOffer, callback) {
        log.silly(`[documentdb/ops/collection] changeOfferType(${offerId})`);
        client.replaceOffer(`offers/${offerId}`, updatedOffer, function (err, replacedOffer) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, (replacedOffer.offerType !== updatedOffer.offerType) ? replacedOffer : null);
            }
        });
    },

    deleteCollection: function (client, databaseRef, collectionId, callback) {
        log.silly(`[documentdb/ops/collection] deleteCollection(${databaseRef.id}, $collectionId)`);
        const link = `${databaseRef._self}${databaseRef._colls}${collectionId}`;
        client.deleteCollection(link, callback);
    }
};
