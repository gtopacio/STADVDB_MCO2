const express = require('express');
const app = express();
const db = require('./db');
require('dotenv').config();

const PORT = process.env.COORDINATOR_PORT || 8000;
app.use(express.urlencoded({extended: true}));
app.use(express.json());

app.post("/query", async(req, res) => {
    const { queries } = req.body;
    let results = await db.executeTransaction(queries);
    res.send(results);
});

app.listen(PORT, () => console.log(`http://localhost:${PORT}`));