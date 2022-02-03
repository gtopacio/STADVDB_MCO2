const db = require('../lib/db');
const generateDatabaseDetails = require('../lib/generateDatabaseDetails');

require('dotenv').config();
const nodeName = process.env.NODE_NAME;
let dbDetails = generateDatabaseDetails(nodeName);
let pool = db.createPool(dbDetails);
let poolPromise = pool.promise();

async function centralHandler({topic, message}) {
    let {rows, timestamp, type} = JSON.parse(message);
    rows = rows[0];
    if(false){ //Timestamp check here
        return;
    }

    if(type === "insert"){
        try{
            let values = [rows.id, rows.name, rows.year, rows.rank];
            await poolPromise.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`) VALUE (?,?,?,?)", values);
        }
        catch(e){
            console.error(e);
        }
        return;
    }

    if(type === "delete"){
        try{
            await poolPromise.execute("DELETE FROM `movies` WHERE `id` = ?", [rows.id]);
        }
        catch(e){
            console.error(e);
        }
        return;
    }

    if(type === "update"){
        try{
            let {before, after} = rows;
            values = [after.name, after.year, after.rank, before.id];
            await poolPromise.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ? WHERE id = ?;", values);
        }
        catch(e){
            console.error(e);
        }
        return;
    }

}

module.exports = centralHandler;