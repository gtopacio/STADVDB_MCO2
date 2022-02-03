require('dotenv').config();
const mysql = require('mysql2');
let pool  = mysql.createPool({
  host            : process.env.CENTRAL_HOSTNAME,
  user            : process.env.COORDINATOR_USERNAME,
  password        : process.env.COORDINATOR_PASSWORD,
  database        : process.env.DATABASE_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});
let poolPromise = pool.promise();

async function ping(){
    let connection = await poolPromise.getConnection();
    return await connection.ping();
}
 
async function executeTransaction({queries, isolationLevel = "REPEATABLE READ"}){
    let connection = await poolPromise.getConnection();
    let results = [];
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`);
    await connection.beginTransaction();
    for(let query of queries){
        try{
            let res = await connection.execute(query);
            results.push(res);
        }
        catch(e){
            await connection.rollback();
            pool.releaseConnection(connection);
            throw e;
        }
    }
    try{
        await connection.commit();
        return results;
    }
    catch(e){
        await connection.rollback();
        throw e;
    }
}

module.exports = {
    executeTransaction, ping
}