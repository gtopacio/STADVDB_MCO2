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

async function insertType({rows, timestamp, origin, connection}){
    for(let row of rows){
        console.log("ATTEMPT INSERT", row);
        let res = await connection.execute("SELECT lastUpdated AS lastUpdated FROM movies WHERE id = ?", [row.id]);
        res = res[0][0];
        let emptyRes = !res;
        if(res && res.lastUpdated)
            res = new Date(res.lastUpdated);
        else
            res = new Date(0);
        if(res > row.lastUpdated) //Timestamp check here
            continue;
        if(res === row.lastUpdated && origin !== 'CENTRAL')
            continue;
        let values = [];
        values.push(row.id);
        values.push(row.name);
        values.push(row.year);
        values.push(row.rank);
        values.push(row.lastUpdated);
        console.log(values);
        try{
            if(emptyRes)
                await connection.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`, `lastUpdated`) VALUE (?,?,?,?,?)", values);
            else
                await connection.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?;", values);
            console.log("Successful INSERT", values, [res]);
        }
        catch(e){
            console.log("Unsuccessful INSERT", values, [res]);
            await connection.rollback();
            pool.releaseConnection(connection);
            return;
        }
    }
    await connection.commit();
    pool.releaseConnection(connection);
    console.log("TRANSACTION COMMITTED");
}

async function deleteType({rows, timestamp, origin, connection}){
    for(let row of rows){
        console.log("ATTEMPT DELETE", row);
        let res = await connection.execute("SELECT lastUpdated FROM movies WHERE id = ?", [row.id]);
        res = res[0][0];
        let timeIssued = new Date(timestamp);
        if(res && res.lastUpdated)
            res = new Date(res.lastUpdated);
        else
            res = new Date(0);
        if(res > timeIssued) //Timestamp check here
            continue;
        if(res === timeIssued && origin !== 'CENTRAL')
            continue;
        try{
            let values = [row.id, row.name, row.year, row.lastUpdated];
            let rankClause = !row.rank ? "IS NULL" : `= ${row.rank}`
            await connection.execute("DELETE FROM `movies` WHERE `id` = ? AND `name` = ? AND `year` = ? AND `rank` " + rankClause + " AND lastUpdated = ?", values);
            console.log("Successful DELETE", values);
        }
        catch(e){
            await connection.rollback();
            pool.releaseConnection(connection);
            console.log("Unsuccessful DELETE", row, [res]);
            return;
        }
    }
    await connection.commit();
    pool.releaseConnection(connection);
    console.log("TRANSACTION COMMITTED");
}

async function updateType({rows, timestamp, connection}){
    for(let row of rows){
        console.log("ATTEMPT UPDATE", row);
        let {before, after} = row;
        let res = await connection.execute("SELECT lastUpdated FROM movies WHERE id = ?", [before.id]);
        res = res[0][0];
        if(res && res.lastUpdated)
            res = new Date(res.lastUpdated);
        else
            res = new Date(0);
        if(res > after.lastUpdated) //Timestamp check here
            continue;
        if(res === after.lastUpdated && origin !== 'CENTRAL')
            continue;
        try{
            let values = [after.name, after.year, after.rank, after.lastUpdated, before.id];
            await connection.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?", values);
            console.log("Successful UPDATE", values, [res]);
        }
        catch(e){
            await connection.rollback();
            pool.releaseConnection(connection);
            console.log("Unsuccessful UPDATE", values, [res]);
            return;
        }
    }
    await connection.commit();
    pool.releaseConnection(connection);
    console.log("TRANSACTION COMMITTED");
}

async function handler(parsedMessage, heartbeat) {
    // let heartbeatInterval = setInterval(heartbeat, 3000);
    let {rows, timestamp, type, origin} = parsedMessage;
    // console.log("Kafka Consumer", {rows, timestamp, type});
    let connection = await poolPromise.getConnection();
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${replicationIsolationLevel};`);
    await connection.execute(`SET sql_log_bin = OFF;`);
    await connection.beginTransaction();
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