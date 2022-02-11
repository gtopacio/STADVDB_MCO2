const db = require('./db');
const multi_db = require('./multi-db.js');
require('dotenv').config();
const host = process.env.NODE_HOSTNAME;
const user = process.env.BACKEND_DB_USERNAME;
const password = process.env.BACKEND_DB_PASSWORD;
const database = process.env.DATABASE_NAME;

let pool = db.createPool({
    host,
    user,
    password,
    database
});

async function executeTransaction(transaction){
    try{
        let res = await multi_db.executeTransaction(transaction);
        return res;
    }
    catch(e){
        throw e;
    }
}

async function ping(){
    try{
      let res = await db.ping(pool);
      return res;
    }
    catch(e){
      console.log(e);
      throw e;
    }
}

module.exports = { ping, executeTransaction };
