const mysql = require('mysql2');
require('dotenv').config();

const poolCentral = mysql.createPool({
    host            : process.env.CENTRAL_HOSTNAME,
    user            : process.env.COORDINATOR_USERNAME,
    password        : process.env.COORDINATOR_PASSWORD,
    database        : process.env.DATABASE_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

const poolL1980 = mysql.createPool({
    host            : process.env.L1980_HOSTNAME,
    user            : process.env.COORDINATOR_USERNAME,
    password        : process.env.COORDINATOR_PASSWORD,
    database        : process.env.DATABASE_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

const poolGE1980 = mysql.createPool({
    host            : process.env.GE1980_HOSTNAME,
    user            : process.env.COORDINATOR_USERNAME,
    password        : process.env.COORDINATOR_PASSWORD,
    database        : process.env.DATABASE_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

const central = poolCentral.promise();
const l1980 = poolL1980.promise();
const ge1980 = poolGE1980.promise();

async function insertStatement({rows}){
    for(let row of rows){
        let values = [];
        values.push(row.id);
        values.push(row.name);
        values.push(row.year);
        values.push(row.rank);
        if(row.year >= 1980){
            await ge1980.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`) VALUE (?,?,?,?)", values);
            continue;
        }
        await l1980.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`) VALUE (?,?,?,?)", values);
    }
}

async function deleteStatement({rows}){
    for(let row of rows){
        let values = [];
        values.push(row.id);
        if(row.year >= 1980){
            await ge1980.execute("DELETE FROM `movies` WHERE `id` = ?", values);
            continue;
        }
        await l1980.execute("DELETE FROM `movies` WHERE `id` = ?", values);
    }
}

async function updateStatement(evt){
    evt.dump();
}

module.exports = {
    insertStatement, deleteStatement, updateStatement
}