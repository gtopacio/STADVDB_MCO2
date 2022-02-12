const db = require('../database/db');

require('dotenv').config();
const replicationIsolationLevel = "REPEATABLE READ";

let dbDetails = {
    host: process.env.NODE_HOSTNAME,
    user: process.env.BACKEND_DB_USERNAME,
    password: process.env.BACKEND_DB_PASSWORD,
    database: process.env.DATABASE_NAME
};

let pool = db.createPool(dbDetails);
let poolPromise = pool.promise();

async function insertType({rows, origin, connection}){
    for(let row of rows){
        console.log("ATTEMPT INSERT", row);
        await connection.beginTransaction();
        let res = await connection.execute("SELECT lastUpdated AS lastUpdated FROM movies WHERE id = ?", [row.id]);
        res = res[0][0];
        let emptyRes = !res;
        res = res && res.lastUpdated ? new Date(res.lastUpdated) : new Date(0);
        if(res > row.lastUpdated){
            await connection.rollback();
            console.log("INSERT: DATA IS OLD")
            continue;
        }
        if((res === row.lastUpdated && origin !== 'CENTRAL')){
            await connection.rollback();
            console.log("INSERT: DATA IS CONCURRENT BUT NOT CENTRAL");
            continue;
        }
        try{
            
            let insertRes;
            if(emptyRes){
                let values = [row.id, row.name, row.year, row.rank, row.lastUpdated];
                insertRes = await connection.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`, `lastUpdated`) VALUE (?,?,?,?,?)", values);
            }    
            else{
                let values = [row.name, row.year, row.rank, row.lastUpdated, row.id];
                insertRes = await connection.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?;", values);
            } 
            insertRes = insertRes[0];
            console.log("Successful INSERT, Affected Rows: ", insertRes.affectedRows);
            await connection.commit();
            console.log("TRANSACTION COMMITTED");
        }
        catch(e){
            console.log("Unsuccessful INSERT");
            await connection.rollback();
        }
    }
    pool.releaseConnection(connection);
    console.log("INSERT JOB END");
}

async function deleteType({rows, timestamp, origin, connection}){
    console.log("START DELETE JOB");
    for(let row of rows){
        console.log("ATTEMPT DELETE", row);
        await connection.beginTransaction();
        let [res] = await connection.execute("SELECT lastUpdated FROM movies WHERE id = ?", [row.id]);
        res = res[0];
        let timeIssued = new Date(timestamp);
        res = res && res.lastUpdated ? new Date(res.lastUpdated) : new Date(0);
        if(res > timeIssued || (res === timeIssued && origin !== 'CENTRAL')){
            await connection.rollback();
            console.log("JOB IS OLD OR CONCURRENT BUT NOT CENTRAL");
            continue;
        }
        try{
            let values = [row.id, row.name, row.year, row.lastUpdated];
            let rankClause = !row.rank ? "IS NULL" : `= ${row.rank}`
            let [deleteRes] = await connection.execute("DELETE FROM `movies` WHERE `id` = ? AND `name` = ? AND `year` = ? AND `rank` " + rankClause + " AND lastUpdated = ?", values);
            console.log("Successful DELETE, Affected Rows: ", deleteRes.affectedRows);
            await connection.commit();
        }
        catch(e){
            await connection.rollback();
            console.log("Unsuccessful DELETE");
        }
    }
    pool.releaseConnection(connection);
    console.log("DELETE JOB END");
}

async function updateType({rows, connection}){
    console.log("START UPDATE JOB");
    for(let row of rows){
        console.log("ATTEMPT UPDATE", row);
        await connection.beginTransaction();
        let {before, after} = row;
        let [res] = await connection.execute("SELECT lastUpdated FROM movies WHERE id = ?", [before.id]);
        res = res[0];
        res = res && res.lastUpdated ? new Date(res.lastUpdated) : new Date(0);
        if(res > after.lastUpdated){
            await connection.rollback();
            console.log("UPDATE: DATA IS OLD")
            continue;
        }
        if((res === after.lastUpdated && origin !== 'CENTRAL')){
            await connection.rollback();
            console.log("UPDATE: DATA IS CONCURRENT BUT NOT CENTRAL");
            continue;
        }
        try{
            let values = [after.name, after.year, after.rank, after.lastUpdated, before.id];
            let [updateRes] = await connection.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?", values);
            console.log("Successful UPDATE, Affected Rows: ", updateRes.affectedRows);
            await connection.commit();
            console.log("Transaction Committed");
        }
        catch(e){
            await connection.rollback();
            console.log("Unsuccessful UPDATE");
        }
    }
    pool.releaseConnection(connection);
    console.log("UPDATE JOB END");
}

async function handler(parsedMessage, heartbeat) {
    // let heartbeatInterval = setInterval(heartbeat, 3000);
    let {rows, timestamp, type, origin} = parsedMessage;
    // console.log("Kafka Consumer", {rows, timestamp, type});
    let connection = await poolPromise.getConnection();
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${replicationIsolationLevel};`);
    await connection.execute(`SET sql_log_bin = OFF;`);
    if(type === "insert"){
        rows = rows.map((row) => {
            row.lastUpdated = new Date(row.lastUpdated);
            return row;
        });
        await insertType({rows, timestamp, origin, connection});
        // clearInterval(heartbeatInterval);
        return;
    }

    if(type === "delete"){
        rows = rows.map((row) => {
            row.lastUpdated = new Date(row.lastUpdated);
            return row;
        });
        await deleteType({rows, timestamp, origin, connection});
        // clearInterval(heartbeatInterval);
        return;
    }

    if(type === "update"){
        rows = rows.map((row) => {
            row.before.lastUpdated = new Date(row.before.lastUpdated);
            row.after.lastUpdated = new Date(row.after.lastUpdated);
            return row;
        });
        await updateType({rows, timestamp, origin, connection});
        // clearInterval(heartbeatInterval);
        return;
    }
}

module.exports = handler;