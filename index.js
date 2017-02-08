const driver = require('./lib/docdbdriver');
const uuid = require('uuid');
const queries = require('azure-mobile-apps').query;

/**
 * Data Provider for Azure Mobile Apps, implementing a DocumentDb backend
 * @param {dataConfiguration} configuration The data block from the configuration
 * @returns {function} A factory function for creating the data provider
 */
module.exports = function (configuration) {
    driver.configure(configuration);

    /**
     * Data Provider definition for a table controller, implementing a DocumentDb
     * backend.
     * @param {tableConfiguration} tableConfig The table configuration
     * @returns {object} Definition of the data provider for a table
     */
    function docdbdriver (tableConfig) {
        return {
            read: function (query) {
                query = query || queries.create(tableConfig.containerName);
                return driver.read(tableConfig, query);
            },

            update: function (item, query) {
                return driver.update(tableConfig, item, query);
            },

            insert: function (item) {
                item.id = item.id || uuid.v4();
                return driver.insert(tableConfig, item);
            },

            delete: function (query, version) {
                return driver.delete(tableConfig, query, version);
            },

            undelete: function (query, version) {
                return driver.undelete(tableConfig, query, version);
            },

            truncate: function () {
                return driver.truncate(tableConfig);
            },

            initialize: function () {
                return driver.initialize(tableConfig);
            },

            schema: function () {
                return driver.schema(tableConfig);
            }
        };
    }

    return docdbdriver;
};
