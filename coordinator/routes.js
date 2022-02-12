const express = require ('express');
const controller = require ('./controllers/controller.js');
const checkCentral = require('./middlewares/checkCentral.js');
const checkSql = require('./middlewares/checkSql.js');
const checkReadOnly = require('./middlewares/checkReadOnly.js');
const app = express();
app.set('views', __dirname+'/views');
app.use(express.static(__dirname + '/public'));

app.get('/', controller.getIndex);
app.post('/query', checkSql, checkReadOnly, checkCentral, controller.query);
app.get("/ping", controller.ping);
app.get("/urls", controller.urls)

module.exports = app;
