const mysql = require('mysql2');
require('dotenv').config();
const user = process.env.BACKEND_DB_USERNAME;
const password = process.env.BACKEND_DB_PASSWORD;
const database = process.env.DATABASE_NAME;

let pools = {
    L1980: mysql.createPool({
        host: process.env.L1980_HOSTNAME,
        user,
        password,
        database,
        connectionLimit: 4,
        queueLimit: 0,
        // acquireTimeout: 5000
    }),
    GE1980: mysql.createPool({
        host: process.env.GE1980_HOSTNAME,
        user,
        password,
        database,
        connectionLimit: 4,
        queueLimit: 0,
        // acquireTimeout: 5000
    })
};

let promisePools = {
    L1980: pools.L1980.promise(),
    GE1980: pools.GE1980.promise()
};

async function requestRebalancing(){
    let { L1980, GE1980 } = promisePools;
    let connL1980 = await L1980.getConnection();
    let connGE1980 = await GE1980.getConnection();
    try{
        console.log("Attempting to Rebalance");
        await connGE1980.execute(`SET sql_log_bin = OFF;`);
        await connL1980.execute(`SET sql_log_bin = OFF;`);
        await requestTableLock({connGE1980, connL1980});
        await Promise.all([
            connGE1980.beginTransaction(),
            connL1980.beginTransaction()
        ]);
        let invalidRows = await getInvalidRows({connGE1980, connL1980});
        await Promise.all([
            insertNewRows({connGE1980, connL1980, invalidRows}),
            deleteInvalidRows({connGE1980, connL1980})
        ]);
        await commit({connGE1980, connL1980});
        await releaseTableLocks({connGE1980, connL1980});
        connL1980.release();
        connGE1980.release();
        console.log("SUCCESSFULLY REBALANCED");
    }
    catch(e){
        await rollback({connGE1980, connL1980});
        await releaseTableLocks({connGE1980, connL1980});
        connL1980.release();
        connGE1980.release();
        console.log("ERROR IN REBALANCING");
        console.error(e);
    }
}

async function requestTableLock({connGE1980, connL1980}){
    try{
        await Promise.all([
            connGE1980.query("LOCK TABLE movies WRITE;"),
            connL1980.query("LOCK TABLE movies WRITE")
        ]);
    }
    catch(e){
        throw e;
    }
}

async function getInvalidRows({connGE1980, connL1980}){
    try{
        let [GEInvalid] = await connGE1980.execute("SELECT *, CENTRAL, L1980, GE1980, tombstone FROM movies WHERE `year` < 1980;");
        let [LInvalid] = await  connL1980.execute("SELECT *, CENTRAL, L1980, GE1980, tombstone FROM movies WHERE `year` >= 1980;");
        return {
            GE1980: GEInvalid,
            L1980: LInvalid
        }
    }
    catch(e){
        throw e;
    }
}

async function deleteInvalidRows({connL1980, connGE1980}){
    try{
        await Promise.all([
            connL1980.execute("UPDATE movies SET tombstone = true WHERE `year` >= 1980;"),
            connGE1980.execute("UPDATE movies SET tombstone = true WHERE `year` < 1980;")
        ]);
    }
    catch(e){
        throw e;
    }
}

async function insertNewRows({connGE1980, connL1980, invalidRows}){
    try{
        let { L1980, GE1980 } = invalidRows;
        for(let value of L1980){
            let values = [value.id, value.name, value.year, value.rank, value.CENTRAL, value.L1980, value.GE1980];
            await connGE1980.execute("INSERT INTO movies (id, `name`, `year`, `rank`, CENTRAL, L1980, GE1980) VALUES (?,?,?,?,?,?,?) AS new ON DUPLICATE KEY UPDATE id = new.id, `name` = new.`name`, `year` = new.`year`, `rank` = new.`rank`, CENTRAL = new.CENTRAL, GE1980 = new.GE1980, L1980 = new.L1980, tombstone = new.tombstone", values);
        }
        for(let value of GE1980){
            let values = [value.id, value.name, value.year, value.rank, value.CENTRAL, value.L1980, value.GE1980];
            await connGE1980.execute("INSERT INTO movies (id, `name`, `year`, `rank`, CENTRAL, L1980, GE1980) VALUES (?,?,?,?,?,?,?) AS new ON DUPLICATE KEY UPDATE id = new.id, `name` = new.`name`, `year` = new.`year`, `rank` = new.`rank`, CENTRAL = new.CENTRAL, GE1980 = new.GE1980, L1980 = new.L1980, tombstone = new.tombstone", values);
        }
    }
    catch(e){
        throw e;
    }
}

async function commit({connGE1980, connL1980}){
    try{
        await Promise.all([
            connGE1980.commit(),
            connL1980.commit()
        ]);
    }
    catch(e){
        throw e;
    }
}

async function releaseTableLocks({connGE1980, connL1980}){
    try{
        await Promise.all([
            connGE1980.query("UNLOCK TABLES;"),
            connL1980.query("UNLOCK TABLES;")
        ])
    }
    catch(e){
        throw e;
    }
}

async function rollback({connGE1980, connL1980}){
    try{
        await Promise.all([
            connGE1980.rollback(),
            connL1980.rollback()
        ]);
    }
    catch(e){
        throw e;
    }
}

module.exports = requestRebalancing;