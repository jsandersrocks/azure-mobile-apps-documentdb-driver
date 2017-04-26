module.exports = {
    createCollection: function (client, pricingTier, databaseRef, collectionId, callback) {
        client.createCollection(databaseRef._self, { id: collectionId }, { offerType: pricingTier }, callback);
    },

    listCollections: function (client, databaseRef, callback) {
        client.readCollections(databaseRef._self).toArray(callback);
    },

    readCollection: function (client, selfLink, callback) {
        client.readCollection(selfLink, callback);
    },

    readCollectionById: function (client, databaseRef, collectionId, callback) {
        client.readCollection(`${databaseRef._self}${databaseRef._colls}${collectionId}`, callback);
    },

    getOfferType: function (client, collection, callback) {
        const querySpec = {
            query: 'SELECT * FROM root r WHERE r.resource = @link',
            parameters: [ { name: '@link', value: collection._self } ]
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
        client.replaceOffer(`offers/${offerId}`, updatedOffer, function (err, replacedOffer) {
            if (err) {
                callback(err, null);
            } else {
                callback(null, (replacedOffer.offerType !== updatedOffer.offerType) ? replacedOffer : null);
            }
        });
    },

    deleteCollection: function (client, databaseRef, collectionId, callback) {
        client.deleteCollection(`${databaseRef._self}${databaseRef._colls}${collectionId}`, callback);
    }
};
