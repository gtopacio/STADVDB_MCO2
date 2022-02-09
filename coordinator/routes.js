const express = require ('express');
const controller = require ('./controllers/controller.js');
const checkCentral = require('./middlewares/checnCentral.js');
const app = express();
app.set('views', __dirname+'/views');

app.get('/', controller.getIndex);
app.post('/query', checkCentral, controller.query);
app.get("/ping", controller.ping);

module.exports = app;
