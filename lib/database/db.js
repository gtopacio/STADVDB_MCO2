const mysql = require('mysql2');

async function ping(pool){
    let poolPromise = pool.promise();
    let connection = await poolPromise.getConnection();
    return await connection.ping();
}
 
async function executeTransaction(pool, {queries, isolationLevel = "REPEATABLE READ"}){
    let poolPromise = pool.promise();
    let connection = await poolPromise.getConnection();
    let results = [];
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel};`);
    await connection.beginTransaction();
    for(let query of queries){
        console.log(query);
        try{
            let res = await connection.execute(query);
            results.push(res[0]);
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