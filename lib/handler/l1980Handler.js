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

async function insertType({rows, timestamp, connection}){
    for(let row of rows){
        if(row.year >= 1980)
            continue;
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
                await poolPromise.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`, `lastUpdated`) VALUE (?,?,?,?,?)", values);
            else
                await poolPromise.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?", values);
            console.log("Successful INSERT", values);
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
    console.log("TRANSACTION END");
}

async function deleteType({rows, timestamp, connection}){
    for(let row of rows){
        if(row.year >= 1980)
            continue;
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
            let values = [row.id, row.name, row.year, row.rank, row.lastUpdated];
            await poolPromise.execute("DELETE FROM `movies` WHERE `id` = ? AND `name` = ? AND `year` = ? AND `rank` = ? AND lastUpdated = ?", values);
            console.log("Successful DELETE", row, [res]);
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
    console.log("TRANSACTION COMMIT");
}

async function updateType({rows, timestamp, connection}){
    for(let row of rows){
        let {before, after} = row;
        let res = await connection.execute("SELECT lastUpdated FROM movies WHERE id = ?", [before.id]);
        res = res[0][0];
        let emptyRes = !res;
        if(res && res.lastUpdated)
            res = new Date(res.lastUpdated);
        else
            res = new Date(0);
        if(res > after.lastUpdated) //Timestamp check here
            continue;
        if(res === after.lastUpdated && origin !== 'CENTRAL')
            continue;

        if(before.year >= 1980 && after.year >= 1980)
            continue;

        if(before.year >= 1980 && after.year < 1980){
            try{
                if(emptyRes){
                    let values = [];
                    values.push(after.id);
                    values.push(after.name);
                    values.push(after.year);
                    values.push(after.rank);
                    values.push(after.lastUpdated);
                    await connection.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`, `lastUpdated`) VALUE (?,?,?,?,?)", values);
                    console.log("Successful UPDATE-INSERT", values);
                }
                else{
                    let values = [after.name, after.year, after.rank, after.lastUpdated, before.id];
                    await connection.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?", values);
                    console.log("Successful UPDATE", values);
                }
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
                let values = [before.id, before.name, before.year, before.rank, before.lastUpdated];
                await poolPromise.execute("DELETE FROM `movies` WHERE `id` = ? AND `name` = ? AND `year` = ? AND `rank` = ? AND lastUpdated = ?", values);
                console.log("Successful UPDATE-DELETE", [before.id]);
            }
            catch(e){
                await connection.rollback();
                pool.releaseConnection(connection);
                return;
            }
            continue;
        }

        if(before.year < 1980 && after.year < 1980){
            try{
                let values = [after.name, after.year, after.rank, after.lastUpdated, before.id];
                await connection.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?", values);
                console.log("Successful UPDATE", values);
            }
            catch(e){
                await connection.rollback();
                pool.releaseConnection(connection);
                return;
            }
        }
    }
    await connection.commit();
    pool.releaseConnection(connection);
    console.log("TRANSACTION COMMIT");
}

async function handler(parsedMessage) {
    let {rows, timestamp, type, origin} = parsedMessage;
    console.log("Kafka Consumer", {rows, timestamp, type});
    let connection = await poolPromise.getConnection();
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${replicationIsolationLevel};`);
    await connection.beginTransaction();
    if(type === "insert"){
        rows = rows.map((row) => {
            row.lastUpdated = new Date(row.lastUpdated);
            return row;
        });
        await insertType({rows, timestamp, origin, connection});
        return;
    }

    if(type === "delete"){
        rows = rows.map((row) => {
            row.lastUpdated = new Date(row.lastUpdated);
            return row;
        });
        await deleteType({rows, timestamp, origin, connection});
        return;
    }

    if(type === "update"){
        rows = rows.map((row) => {
            row.before.lastUpdated = new Date(row.before.lastUpdated);
            row.after.lastUpdated = new Date(row.after.lastUpdated);
            return row;
        });
        await updateType({rows, timestamp, origin, connection});
        return;
    }
}

module.exports = handler;