/*
    Coordinator accepts requests and routes to the correct route
    if needed to be rerouted;
*/

const express = require('express');
const app = express();
const cors = require('cors');
const routes = require('./routes.js');

require('dotenv').config();
app.set('view engine', 'hbs');
app.use(cors());
const PORT = process.env.COORDINATOR_PORT || 8000;
const NODE_HOSTNAME = process.env.NODE_HOSTNAME;
let server;

function start(){
    app.use(express.urlencoded({extended: true}));
    app.use(express.json());
    app.use('/', routes);
    server = require('http').createServer(app);
    server.listen(PORT, () => console.log(`http://${NODE_HOSTNAME}:${PORT}`));
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
