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

async function insertType({rows, connection}){
    console.log("START INSERT JOB");
    for(let row of rows){
        console.log("ATTEMPT", row);
        if(row.year >= 1980){
            console.log("INSERT: ROW NOT IN DATA SCOPE");
            continue;
        }
        
        await connection.beginTransaction();


        let died = await connection.execute("SELECT died FROM graveyard WHERE id = ? FOR UPDATE", [row.id]);
        died = died[0][0];

        if(died && died > row.lastUpdated){
            console.log("INSERT: Unsuccessful, necromancy detected...");
            await connection.rollback();
            continue;
        }

        let res = await connection.execute("SELECT lastUpdated AS lastUpdated FROM movies WHERE id = ? FOR UPDATE", [row.id]);
        res = res[0][0];
        let emptyRes = !res;
        res = res && res.lastUpdated ? new Date(res.lastUpdated) : new Date(0);

        if(res > row.lastUpdated){
            console.log("INSERT: ROW IS OLD");
            await connection.rollback();
            continue;
        }
            
        if(res === row.lastUpdated && origin !== 'CENTRAL'){
            console.log("INSERT: CONCURRENT BUT NOT CENTRAL");
            await connection.rollback();
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
                insertRes = await connection.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?", values);
            }
            insertRes = insertRes[0];
            console.log("Successful INSERT, Affected Rows: ", insertRes.affectedRows);
            if(died){
                await connection.execute("DELETE FROM graveyard WHERE id = ?", [row.id]);
                console.log("INSERT: Row was resurrected");
            }
            await connection.commit();
            console.log("TRANSACTION COMMITTED");
            continue;
        }
        catch(e){
            await connection.rollback();
            console.log("INSERT: ERROR IN INSERTING OR UPDATING");
            continue;
        }
    }

    connection.release();
    console.log("INSERT JOB END");
}

async function deleteType({rows, timestamp, connection}){
    console.log("START DELETE JOB");
    for(let row of rows){
        console.log("ATTEMPT", row);
        if(row.year >= 1980){
            console.log("DELETE: DATA NOT IN SCOPE");
            continue;
        }

        await connection.beginTransaction();
        let res = await connection.execute("SELECT lastUpdated FROM movies WHERE id = ?", [row.id]);
        res = res[0][0];
        let timeIssued = new Date(timestamp);
        res = res && res.lastUpdated ? new Date(res.lastUpdated) : new Date(0);

        if(res > timeIssued){
            console.log("DELETE: TASK IS OLDER THAN EXISTING DATA");
            await connection.rollback();
            continue;
        }

        if(res === timeIssued && origin !== 'CENTRAL'){
            console.log("DELETE: CONCURRENT BUT NOT CENTRAL");
            await connection.rollback();
            continue;
        }

        let died = await connection.execute("SELECT died FROM graveyard WHERE id = ? FOR UPDATE", [row.id]);
        died = died[0][0];

        try{
            let values = [row.id, row.name, row.year, row.lastUpdated];
            let rankClause = !row.rank ? "IS NULL" : `= ${row.rank}`
            let [deleteRes] = await connection.execute("DELETE FROM `movies` WHERE `id` = ? AND `name` = ? AND `year` = ? AND `rank` " + rankClause + " AND lastUpdated = ?", values);
            console.log("Successful DELETE, Attempted Rows: ", deleteRes.affectedRows);

            if(died){
                await connection.execute("UPDATE graveyard SET died = ? WHERE id = ?", [timeIssued, row.id]);
            }
            else{
                await connection.execute("INSERT INTO graveyard(id, died) VALUES (?,?)", [row.id, timeIssued]);
            }
            console.log("DELETE: Death Recorded");

            await connection.commit();
            console.log("TRANSACTION COMMITTED");
            continue;
        }
        catch(e){
            await connection.rollback();
            console.log("DELETE: ERROR IN SQL");
            continue;
        }
    }
    connection.release();
    console.log("DELETE JOB END");
}

async function updateType({rows, timestamp, connection}){
    console.log("START UPDATE JOB");
    for(let row of rows){
        console.log("ATTEMPT", row);
        let {before, after} = row;

        if(before.year >= 1980 && after.year >= 1980){
            console.log("Unsuccessful UPDATE (OUT OF SCOPE hard ge1980 data)");
            continue;
        }

        await connection.beginTransaction();
        let res = await connection.execute("SELECT lastUpdated FROM movies WHERE id = ?", [before.id]);
        res = res[0][0];
        let emptyRes = !res;
        res = res && res.lastUpdated ? new Date(res.lastUpdated) : new Date(0);

        if(res > after.lastUpdated){
            console.log("Unsuccessful UPDATE (DATA IS OLD)");
            await connection.rollback();
            continue;
        }
            
        if(res === after.lastUpdated && origin !== 'CENTRAL'){
            console.log("Unsuccessful UPDATE (CENTRAL WON)");
            await connection.rollback();
            continue;
        }

        let died = await connection.execute("SELECT died FROM graveyard WHERE id = ? FOR UPDATE", [before.id]);
        died = died[0][0];
        console.log("UPDATE: Grave Digging");

        if(before.year >= 1980 && after.year < 1980){
            try{
                if(emptyRes){

                    if(died && died > after.lastUpdated){
                        console.log("INSERT: Unsuccessful, necromancy detected...");
                        await connection.rollback();
                        continue;
                    }

                    let values = [after.id, after.name, after.year, after.rank, after.lastUpdated];
                    let [updateRes] = await connection.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`, `lastUpdated`) VALUE (?,?,?,?,?)", values);
                    console.log("Successful UPDATE-INSERT, Affected Rows: ", updateRes.affectedRows);

                    if(died){
                        await connection.execute("DELETE FROM graveyard WHERE id = ?", [before.id]);
                        console.log("UPDATE-INSERT: Row was resurrected");
                    }

                }
                else{
                    let values = [after.name, after.year, after.rank, after.lastUpdated, before.id];
                    let [updateRes] = await connection.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?;", values);
                    console.log("Successful UPDATE, Affected Rows: ", updateRes.affectedRows);
                }

                await connection.commit();
                console.log("TRANSACTION COMMITTED");
                continue;
            }
            catch(e){
                await connection.rollback();
                console.log("Unsuccessful UPDATE ge -> l");
                continue;
            }
        }

        if(before.year < 1980 && after.year > 1980){
            try{
                let values = [before.id, before.name, before.year, before.lastUpdated];
                let rankClause = !before.rank ? "IS NULL" : `= ${before.rank}`
                let [updateRes] = await connection.execute("DELETE FROM `movies` WHERE `id` = ? AND `name` = ? AND `year` = ? AND `rank` " + rankClause + " AND lastUpdated = ?", values);
                console.log("Successful UPDATE-DELETE, Affected Rows: ", updateRes.affectedRows);

                if(died){
                    await connection.execute("UPDATE graveyard SET died = ? WHERE id = ?", [after.lastUpdated, before.id]);
                }
                else{
                    await connection.execute("INSERT INTO graveyard(id, died) VALUES (?,?)", [before.id, after.lastUpdated]);
                }
                console.log("DELETE: Death Recorded");
                await connection.commit();
                console.log("TRANSACTION COMMITTED");
                continue;
            }
            catch(e){
                await connection.rollback();
                console.log("Unsuccessful UPDATE l -> ge");
                continue;
            }
        }

        if(before.year < 1980 && after.year < 1980){
            try{
                let values = [after.name, after.year, after.rank, after.lastUpdated, before.id];
                let [updateRes] = await connection.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, lastUpdated = ? WHERE id = ?;", values);
                console.log("Successful UPDATE, Affected Rows: ", updateRes.affectedRows);
                await connection.commit();
                console.log("TRANSACTION COMMITTED");
                continue;
            }
            catch(e){
                await connection.rollback();
                console.log("Unsuccessful UPDATE hard l1980 data");
                continue;
            }
        }
    }

    connection.release();
    console.log("UPDATE JOB END");
}

async function handler(parsedMessage) {
    // let heartbeatInterval = setInterval(heartbeat, 3000);
    let {rows, timestamp, type, origin} = parsedMessage;
    // console.log("Kafka Consumer", {rows, timestamp, type});
    let connection = await poolPromise.getConnection();
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${replicationIsolationLevel};`);
    await connection.execute(`SET sql_log_bin = OFF`);

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