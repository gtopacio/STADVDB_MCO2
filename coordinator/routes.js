const express = require ('express');
const controller = require ('./controllers/controller.js');
const checkCentral = require('./middlewares/checkCentral.js');
const checkSql = require('./middlewares/checkSql.js');
const checkReadOnly = require('./middlewares/checkReadOnly.js');
const attachClock = require('./middlewares/attachClock');
const app = express();
app.set('views', __dirname+'/views');
app.use(express.static(__dirname + '/public'));

app.get('/', controller.getIndex);
app.post('/query', checkCentral, checkSql, checkReadOnly, attachClock, controller.query);
app.get("/ping", controller.ping);
app.get("/urls", controller.urls)

module.exports = app;
