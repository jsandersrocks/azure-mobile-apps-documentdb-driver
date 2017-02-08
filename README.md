# Azure Mobile Apps DocumentDb Driver

This contains the Azure Mobile Apps DocumentDb Driver for the Node SDK.

## Configuring via App Settings

Set a Connection String called `MS_DocumentDbConnectionString` to the connection
string of your DocumentDb instance.  This is found in the Keys section of the
DocumentDb resource.

## Configuring in azureMobile.js

Here is an example azureMobile.js file:

```javascript
var docdb = require('azure-mobile-apps-documentdb-driver');
var winston = require('winston');

module.exports = {
    data: {
        provider: docdb,
        host: '<the url to your documentdb instance>',
        accountKey: '<the account key for your documentdb instance>'
    },

    logging: {
        level: 'silly',
        transports: [
            new winston.transports.Console({ colorize: true, timestamp: true })
        ]
    }
};
```

## Configuring in code

You can configure within the your code as well, like this:

```javascript
const mobile = zumo({
    data: {
        provider: require('azure-mobile-apps-documentdb-driver')
    }
});
```

This is useful when configuring via App Settings as you still have to tell the
server to use the driver!

## Other Options in the data object

* database: 'AzureMobile',
* connectionPolicy: undefined,
* consistencyLevel: 'Session',
* pricingTier: 'S1'

These have specific meanings within DocumentDb.  They have the same meanings here.
