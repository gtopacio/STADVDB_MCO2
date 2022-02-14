const db = require('./db');
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
    console.log("LOCAL DB IN");
    try{
        let res = await db.executeTransaction(pool, transaction);
        console.log("TRANSACTION EXECUTED");
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

async function getConnection(){
    return await pool().promise().getConnection();
}

module.exports = { ping, executeTransaction, getConnection };
