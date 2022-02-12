const express = require ('express');
const controller = require ('./controllers/controller.js');
const checkCentral = require('./middlewares/checkCentral.js');
const checkSql = require('./middlewares/checkSql.js');
const checkReadOnly = require('./middlewares/checkReadOnly.js');
const app = express();
app.set('views', __dirname+'/views');
app.use(express.static(__dirname + '/public'));

app.get('/', controller.getIndex);
app.post('/query', checkCentral, checkSql, checkReadOnly, controller.query);
app.get("/ping", controller.ping);

module.exports = app;
