const db = require('../database/db');
const vclock = require('../vectorclock');

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
        if(row.year < 1980){
            console.log("INSERT: ROW NOT IN DATA SCOPE");
            continue;
        }
        
        await connection.beginTransaction();

        let newClock = { clock: { CENTRAL: row.CENTRAL, GE1980: row.GE1980, L1980: row.L1980 }};

        let res = await connection.execute("SELECT CENTRAL, L1980, GE1980, tombstone FROM movies WHERE id = ? FOR UPDATE", [row.id]);
        res = res[0][0];
        let tombstone = false;
        let recordedClock = { clock: {CENTRAL: 0, GE1980: 0, L1980: 0 } };
        if(res){
            tombstone = res.tombstone;
            recordedClock.clock = { CENTRAL: res.CENTRAL, GE1980: res.GE1980, L1980: res.L1980 };
        }

        if(vectorClock.compare(recordedClock, newClock) == 1){
            console.log("INSERT: ROW IS OLD");
            await connection.rollback();
            continue;
        }
            
        if(vectorClock.isConcurrent(recordedClock, newClock) && origin !== 'CENTRAL'){
            console.log("INSERT: CONCURRENT BUT NOT CENTRAL");
            await connection.rollback();
            continue;
        }
            
        
        try{
            let insertRes;
            if(!res){
                let values = [row.id, row.name, row.year, row.rank, row.CENTRAL, row.L1980, row.GE1980];
                insertRes = await connection.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`, `CENTRAL`, `L1980`, `GE1980`) VALUE (?,?,?,?,?,?,?)", values);
            }  
            else{
                let values = [row.name, row.year, row.rank, row.CENTRAL, row.L1980, row.GE1980, row.id];
                insertRes = await connection.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, CENTRAL = ?, L1980 = ?, GE1980 = ?, tombstone = false WHERE id = ?", values);
            }
            insertRes = insertRes[0];
            console.log("Successful INSERT, Affected Rows: ", insertRes.affectedRows);
            console.log(`ID: ${row.id}, New Clock: ${newClock.clock}, Last Update: ${recordedClock.clock}`);
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

async function updateType({rows, connection}){
    console.log("START UPDATE JOB");
    for(let row of rows){
        console.log("ATTEMPT", row);
        let {before, after} = row;
        
        if(before.year < 1980 && after.year < 1980){
            console.log("UPDATE: Data out of scope");
            continue;
        }

        try{
            await connection.beginTransaction();
            let storedRecord = await connection.execute("SELECT CENTRAL, L1980, GE1980, tombstone FROM movies WHERE id = ? FOR UPDATE", [row.id]);
            storedRecord = storedRecord[0][0];
            let recordedTombstone = false;
            let recordedClock = { clock: {CENTRAL: 0, GE1980: 0, L1980: 0 } };
            if(storedRecord){
                recordedTombstone = storedRecord.tombstone;
                recordedClock.clock = { CENTRAL: storedRecord.CENTRAL, GE1980: storedRecord.GE1980, L1980: storedRecord.L1980 };
            }

            let newClock = { clock: { CENTRAL: after.CENTRAL, L1980: after.L1980, GE1980: after.GE1980} };

            if(vectorClock.compare(recordedClock, newClock) == 1){
                console.log("UPDATE: Row is old");
                await connection.rollback();
                continue;
            }
                
            if(vectorClock.isConcurrent(recordedClock, newClock) && origin !== 'CENTRAL'){
                console.log("UPDATE: Concurrent but not Central");
                await connection.rollback();
                continue;
            }

            let newTombstone = after.tombstone;

            if(!newTombstone && before.year >= 1980 && after.year < 1980){
                newTombstone = true;
            }

            if(!storedRecord){
                let values = [row.id, row.name, row.year, row.rank, row.CENTRAL, row.L1980, row.GE1980, newTombstone];
                insertRes = await connection.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`, `CENTRAL`, `L1980`, `GE1980`, tombstone) VALUE (?,?,?,?,?,?,?,?)", values);
            }  
            else{
                let values = [row.name, row.year, row.rank, row.CENTRAL, row.L1980, row.GE1980, newTombstone, row.id];
                insertRes = await connection.execute("UPDATE movies SET `name` = ?, `year` = ?, `rank` = ?, CENTRAL = ?, L1980 = ?, GE1980 = ?, tombstone = ? WHERE id = ?", values);
            }

        }
        catch(e){
            await connection.rollback();
            console.log("UPDATE: Error thrown");
            console.error(e);
            continue;
        }

    }

    connection.release();
    console.log("UPDATE JOB END");
}

async function handler(parsedMessage) {
    let {rows, clock, type, origin} = parsedMessage;
    vclock.increment();
    let mergedClock = vclock.merge(clock);
    let connection = await poolPromise.getConnection();
    await connection.execute(`SET TRANSACTION ISOLATION LEVEL ${replicationIsolationLevel};`);
    await connection.execute(`SET sql_log_bin = OFF`);
    if(type === "insert"){
        await insertType({rows, mergedClock, origin, connection});
        return;
    }

    // if(type === "delete"){
    //     rows = rows.map((row) => {
    //         row.lastUpdated = new Date(row.lastUpdated);
    //         return row;
    //     });
    //     await deleteType({rows, timestamp, origin, connection});
    //     return;
    // }

    if(type === "update"){
        await updateType({rows, mergedClock, origin, connection});
        return;
    }
}

module.exports = handler;

//  async function deleteType({rows, timestamp, connection}){
    //     console.log("START DELETE JOB");
    //     for(let row of rows){
    //         console.log("ATTEMPT", row);
    //         if(row.year < 1980){
    //             console.log("DELETE: DATA NOT IN SCOPE");
    //             continue;
    //         }
    //         await connection.beginTransaction();
            
    //         let tombstone = await connection.execute("SELECT died FROM graveyard WHERE id = ? FOR UPDATE", [row.id]);
    //         tombstone = tombstone[0][0];
    //         tombstone = tombstone ? new Date(tombstone.died) : null;
    
    //         let res = await connection.execute("SELECT lastUpdated FROM movies WHERE id = ?", [row.id]);
    //         res = res[0][0];
    //         let timeIssued = new Date(timestamp);
    //         res = res && res.lastUpdated ? new Date(res.lastUpdated) : new Date(0);
    
    //         if(res > timeIssued){
    //             console.log("DELETE: TASK IS OLDER THAN EXISTING DATA");
    //             await connection.rollback();
    //             continue;
    //         }
    //         if(res === timeIssued && origin !== 'CENTRAL'){
    //             console.log("DELETE: CONCURRENT BUT NOT CENTRAL");
    //             await connection.rollback();
    //             continue;
    //         }
    //         try{
    //             let values = [row.id, row.name, row.year, row.lastUpdated];
    //             let rankClause = !row.rank ? "IS NULL" : `= ${row.rank}`
    //             let [deleteRes] = await connection.execute("DELETE FROM `movies` WHERE `id` = ? AND `name` = ? AND `year` = ? AND `rank` " + rankClause + " AND lastUpdated = ?", values);
    //             console.log("Successful DELETE, Affected Rows: ", deleteRes.affectedRows);
    //             if(tombstone){
    //                 await connection.execute("UPDATE graveyard SET died = ? WHERE id = ?", [timeIssued, row.id]);
    //             }
    //             else{
    //                 await connection.execute("INSERT INTO graveyard(id, died) VALUES (?,?)", [row.id, timeIssued]);
    //             }
    //             console.log("DELETE: Death Recorded");
    //             await connection.commit();
    //             console.log("TRANSACTION COMMITTED");
    //         }
    //         catch(e){
    //             await connection.rollback();
    //             console.log("DELETE: ERROR IN SQL");
    //         }
    //     }
        
    //     connection.release();
    //     console.log("DELETE JOB END");
    // }