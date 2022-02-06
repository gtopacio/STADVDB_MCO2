const express = require ('express');
const controller = require ('./controllers/controller.js');
const app = express();
app.set('views', __dirname+'/views');

app.get('/', controller.getIndex);
app.post('/query', controller.query);

module.exports = app;
