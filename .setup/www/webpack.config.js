// this is to load env vars for this config
require('dotenv').config({ // it puts the content to the "process.env" var. System vars are taking precedence
    path: '.env',
});
// and this to pass env vars to the JS application
const DotenvPlugin = require('webpack-dotenv-plugin');

module.exports = {
 entry: 'index.js',
 mode: 'production',
 plugins: [
    // ...
    new DotenvPlugin({ // makes vars available to the application js code
        path: '.env',
        sample: '.env',
        allowEmptyValues: true,
    }),
    // ...
 ]
};