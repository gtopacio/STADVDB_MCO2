/*
    Coordinator accepts requests and routes to the correct route
    if needed to be rerouted;
*/

const express = require('express');
const app = express();
const hbs = require('hbs');
const routes = require('./routes.js');

require('dotenv').config();
app.set('view engine', 'hbs');
const PORT = process.env.COORDINATOR_PORT || 8000;
let server;

function start(){
    app.use(express.urlencoded({extended: true}));
    app.use(express.json());
    app.use('/', routes);
    server = require('http').createServer(app);
    server.listen(PORT, () => console.log(`http://localhost:${PORT}`));
}

function stop(){
    try{
        server.close();
    }
    catch(e){
        console.error(e);
    }
}

module.exports = { start, stop };
