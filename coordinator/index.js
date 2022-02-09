/*
    Coordinator accepts requests and routes to the correct route
    if needed to be rerouted;
*/

const express = require('express');
const db = require('../lib/database/localDatabase');
const app = express();
require('dotenv').config();
const PORT = process.env.COORDINATOR_PORT || 8000;
let server;

function start(){
    app.use(express.urlencoded({extended: true}));
    app.use(express.json());
    app.get("/ping", (req, res) => {
        res.send("success");
    });
    app.post("/query", async(req, res) => {
        try{
            let results = await db.executeTransaction(req.body);
            res.send(results);
        }
        catch(e){
            console.error(e);
            res.send({e});
        }
    });
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