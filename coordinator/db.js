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

function ping(){
    pool.getConnection().ping((err) => {
        if(err) throw err;
        console.log("success");
    });
}
 
async function executeTransaction(queries){
    let poolPromise = pool.promise();
    let connection = await poolPromise.getConnection();
    let promises = queries.map(query => connection.query(query));
    await connection.beginTransaction();
    let results = await Promise.all(promises);
    try{
        await connection.commit();
    }
    catch{
        console.log("rollbacked");
        await connection.rollback();
    }
    finally{
        pool.releaseConnection(connection);
        return results;
    }
}

module.exports = {
    executeTransaction, ping
}