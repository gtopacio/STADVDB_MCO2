const db = require('./lib/database/db');
const generateDatabaseDetails = require('./lib/generateDatabaseDetails');
const databaseDetails = generateDatabaseDetails('central');
let pool = db.createPool(databaseDetails);

async function executeTransaction(transaction){
    return await db.executeTransaction(pool, transaction);
}

async function ping(){
    return await db.ping(pool);
}

async function start(){
    let pingResult = await ping();
    console.log(pingResult);
}

module.exports = {
    executeTransaction,
    ping,
    start
}