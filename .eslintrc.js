module.exports = {
    "extends": "standard",
    "installedESLint": true,
    "plugins": [
        "standard",
        "promise"
    ],
    "rules": {
        "indent": [ "warn", 4, {
            SwitchCase: 1
        } ],
        "one-var": [ "warn", "never" ],
        "semi": [ "error", "always" ]
    }
};