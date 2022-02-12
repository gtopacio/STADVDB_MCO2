const mysql = require('mysql2');

async function ping(pool){
    try{
        let poolPromise = pool.promise();
        let connection = await poolPromise.getConnection();
        let res = await connection.ping();
        pool.releaseConnection(connection);
        return res;
    }
    catch(e){
        throw e;
    }
}
 
async function executeTransaction(pool, {queries, isolationLevel = "REPEATABLE READ"}){
    let poolPromise = pool.promise();
    let connection = await poolPromise.getConnection();
    let results = [];
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`);
    console.log("ISOLATION LEVEL SET TO " + isolationLevel);
    await connection.beginTransaction();
    console.log("BEGIN TRANSACTION");
    for(let query of queries){
        console.log(query);
        try{
            let res = await connection.execute(query);
            results.push(res[0]);
            console.log("EXECUTED " + query);
        }
        catch(e){
            await connection.rollback();
            pool.releaseConnection(connection);
            console.log("FAILED " + query);
            throw e;
        }
    }
    try{
        await connection.commit();
        console.log("COMMITTED");
        pool.releaseConnection(connection);
        return results;
    }
    catch(e){
        await connection.rollback();
        pool.releaseConnection(connection);
        throw e;
    }
}

function createPool({host, user, password, database}){
    let pool = mysql.createPool({
        host,
        user,
        password,
        database,
        waitForConnections: true,
        connectionLimit: 10,
        queueLimit: 0
    });
    return pool;
}

module.exports = {
    executeTransaction, ping, createPool
}