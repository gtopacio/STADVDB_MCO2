/*
    Coordinator accepts requests and routes to the correct route
    if needed to be rerouted;
*/

const express = require('express');
const db = require('../lib/db.js');
const generateDatabaseDetails = require('../lib/generateDatabaseDetails');
const app = express();
require('dotenv').config();

const PORT = process.env.COORDINATOR_PORT || 8000;
const nodeName = process.env.NODE_NAME;
const databaseDetails = generateDatabaseDetails(nodeName);

app.use(express.urlencoded({extended: true}));
app.use(express.json());

app.post("/query", async(req, res) => {
    try{
        let pool = db.createPool(databaseDetails);
        let results = await db.executeTransaction(pool, req.body);
        res.send(results);
    }
    catch(e){
        console.error(e);
        res.send({e});
    }
});

app.listen(PORT, () => console.log(`http://localhost:${PORT}`));