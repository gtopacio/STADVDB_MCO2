const express = require ('express');
const controller = require ('./controllers/controller.js');
const checkCentral = require('./middlewares/checkCentral.js');
const checkSql = require('./middlewares/checkSql.js');
const checkReadOnly = require('./middlewares/checkReadOnly.js');
const modifySQL = require('./middlewares/modifySQL');
const redirectedCheckCentral = require('./middlewares/redirectedCheckCentral');
const app = express();
app.set('views', __dirname+'/views');
app.use(express.static(__dirname + '/public'));

app.get('/', controller.getIndex);
app.post('/query', checkCentral, checkSql, modifySQL, checkReadOnly, controller.query);
app.post('/query/redirected', redirectedCheckCentral, checkSql, modifySQL, checkReadOnly, controller.query);
app.get("/ping", controller.ping);
app.get("/urls", controller.urls)

module.exports = app;
