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

async function insertType({rows, timestamp}){
    let connection = await poolPromise.getConnection();
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${replicationIsolationLevel};`);
    await connection.beginTransaction();
    for(let row of rows){
        if(row.year >= 1980)
            continue;
        let values = [];
        values.push(row.id);
        values.push(row.name);
        values.push(row.year);
        values.push(row.rank);
        values.push(row.lastUpdated);
        try{
            await connection.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`, `lastUpdated`) VALUE (?,?,?,?,?)", values);
        }
        catch(e){
            console.error(e);
            await connection.rollback();
            pool.releaseConnection(connection);
            return;
        }
    }
    await connection.commit();
    pool.releaseConnection(connection);
}

async function deleteType({rows, timestamp}){
    let connection = await poolPromise.getConnection();
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${replicationIsolationLevel};`);
    await connection.beginTransaction();
    for(let row of rows){
        if(row.year >= 1980)
            continue;
        try{
            await connection.execute("DELETE FROM `movies` WHERE `id` = ?", [row.id]);
        }
        catch(e){
            console.error(e);
            await connection.rollback();
            pool.releaseConnection(connection);
            return;
        }
    }
    await connection.commit();
    pool.releaseConnection(connection);
}

async function updateType({rows, timestamp}){
    let connection = await poolPromise.getConnection();
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${replicationIsolationLevel};`);
    await connection.beginTransaction();
    for(let row of rows){
        let {before, after} = row;

        if(before.year >= 1980 && after.year >= 1980)
            continue;

        if(before.year >= 1980 && after.year < 1980){
            let values = [];
            values.push(after.id);
            values.push(after.name);
            values.push(after.year);
            values.push(after.rank);
            values.push(after.lastUpdated);
            try{
                await connection.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`, `lastUpdated`) VALUE (?,?,?,?,?)", values);
            }
            catch(e){
                console.error(e);
                await connection.rollback();
                pool.releaseConnection(connection);
                return;
            }
            continue;
        }

        if(before.year < 1980 && after.year >= 1980){
            try{
                await connection.execute("DELETE FROM `movies` WHERE `id` = ?", [before.id]);
            }
            catch(e){
                console.error(e);
                await connection.rollback();
                pool.releaseConnection(connection);
                return;
            }
            continue;
        }

        if(before.year < 1980 && after.year < 1980){
            try{
                values = [after.name, after.year, after.rank, before.id, after.lastUpdated];
                await poolPromise.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?;", values);
            }
            catch(e){
                console.error(e);
                await connection.rollback();
                pool.releaseConnection(connection);
                return;
            }
        }
    }
    await connection.commit();
    pool.releaseConnection(connection);
}

async function handler({topic, partition, message}) {
    let {rows, timestamp, type} = JSON.parse(message.value);
    console.log("Kafka Consumer", {rows, timestamp, type});
    if(type === "insert"){
        await insertType({rows, timestamp});
        return;
    }

    if(type === "delete"){
        await deleteType({rows, timestamp});
        return;
    }

    if(type === "update"){
        await updateType({rows, timestamp});
        return;
    }
}

module.exports = handler;