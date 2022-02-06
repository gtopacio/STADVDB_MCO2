const express = require('express');
const app = express();
const db = require('./db');
const hbs = require('hbs');
const routes = require('./routes.js');
require('dotenv').config();

app.set('view engine', 'hbs');

const PORT = process.env.COORDINATOR_PORT || 8000;
app.use(express.urlencoded({extended: true}));
app.use(express.json());

app.use('/', routes);

app.listen(PORT, () => console.log(`http://localhost:${PORT}`));
