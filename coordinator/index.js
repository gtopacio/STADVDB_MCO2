const express = require('express');
const db = require('../lib/dbCentral.js');
const app = express();
require('dotenv').config();

const PORT = process.env.COORDINATOR_PORT || 8000;
app.use(express.urlencoded({extended: true}));
app.use(express.json());

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

app.listen(PORT, () => console.log(`http://localhost:${PORT}`));