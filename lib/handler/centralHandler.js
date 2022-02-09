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
        let res = await connection.execute("SELECT unix_timestamp(lastUpdated) AS lastUpdated FROM movies WHERE id = ?", [row.id]);
        res = res[0][0];
        let emptyRes = !res;
        if(res && res.lastUpdated)
            res = res.lastUpdated;
        else
            res = -1;
        if(res > row.lastUpdated) //Timestamp check here
            continue;
        if(res === row.lastUpdated && origin !== 'CENTRAL')
            continue;
        let values = [];
        values.push(row.id);
        values.push(row.name);
        values.push(row.year);
        values.push(row.rank);
        values.push(new Date(row.lastUpdated*1000));
        console.log(values);
        try{
            if(emptyRes)
                await poolPromise.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`, `lastUpdated`) VALUE (?,?,?,?,?)", values);
            else
                await poolPromise.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?;", values);
            await connection.rollback();
            pool.releaseConnection(connection);
            console.log("Successful INSERT", values, [res]);
            return;
        }
        catch(e){
            console.error(e);
            await connection.rollback();
            pool.releaseConnection(connection);
            console.log("Unsuccessful INSERT", values, [res]);
            return;
        }
    }
    await connection.commit();
    pool.releaseConnection(connection);
    console.log("TRANSACTION END");
}

async function deleteType({rows, timestamp, origin, connection}){
    for(let row of rows){
        let res = await connection.execute("SELECT unix_timestamp(lastUpdated) AS lastUpdated FROM movies WHERE id = ?", [row.id]);
        res = res[0][0];
        let timeIssued = timestamp * 0.001;
        if(res && res.lastUpdated)
            res = res.lastUpdated;
        else
            res = -1;
        if(res > timeIssued) //Timestamp check here
            continue;
        if(res === timeIssued && origin !== 'CENTRAL')
            continue;
        try{
            // await poolPromise.execute("DELETE FROM `movies` WHERE `id` = ?", [row.id]);
            await connection.rollback();
            pool.releaseConnection(connection);
            console.log("Successful DELETE", values, [res]);
            return;
        }
        catch(e){
            console.error(e);
            await connection.rollback();
            pool.releaseConnection(connection);
            console.log("Unsuccessful DELETE", values, [res]);
            return;
        }
    }
    await connection.commit();
    pool.releaseConnection(connection);
    console.log("TRANSACTION END");
}

async function updateType({rows, timestamp, connection}){
    for(let row of rows){
        let {before, after} = row;
        let res = await connection.execute("SELECT unix_timestamp(lastUpdated) AS lastUpdated FROM movies WHERE id = ?", [before.id]);
        res = res[0][0];
        if(res && res.lastUpdated)
            res = res.lastUpdated;
        else
            res = -1;
        if(res > after.lastUpdated) //Timestamp check here
            continue;
        if(res === after.lastUpdated && origin !== 'CENTRAL')
            continue;
        try{
            let values = [after.name, after.year, after.rank, new Date(after.lastUpdated), before.id];
            await poolPromise.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?", values);
            console.log("Successful UPDATE", values, [res]);
        }
        catch(e){
            console.error(e);
            await connection.rollback();
            pool.releaseConnection(connection);
            console.log("Unsuccessful UPDATE", values, [res]);
            return;
        }
    }
    await connection.commit();
    pool.releaseConnection(connection);
    console.log("TRANSACTION END");
}

async function handler(parsedMessage) {
    let {rows, timestamp, type, origin} = parsedMessage;
    console.log("HANDLER");
    let connection = await poolPromise.getConnection();
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${replicationIsolationLevel};`);
    await connection.beginTransaction();
    if(type === "insert"){
        await insertType({rows, timestamp, origin, connection});
        return;
    }

    if(type === "delete"){
        await deleteType({rows, timestamp, origin, connection});
        return;
    }

    if(type === "update"){
        await updateType({rows, timestamp, origin, connection});
        return;
    }
}

module.exports = handler;