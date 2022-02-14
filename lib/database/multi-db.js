const mysql = require('mysql2');
const { Parser } = require('node-sql-parser');
const parser = new Parser();
require('dotenv').config();
const generateSubTransactions = require('./generateSubTransactions');
const requestRebalancing = require('./requestRebalancing');

const user = process.env.BACKEND_DB_USERNAME;
const password = process.env.BACKEND_DB_PASSWORD;
const database = process.env.DATABASE_NAME;
const tempName = "tempTable";

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

async function readOnlyTransaction({connGE1980, connL1980, queries, isolationLevel}){
    try{
        await setIsolationLevel({connGE1980, connL1980, isolationLevel});
        await beginTransactions({connGE1980, connL1980});
        let transactionResults = [];
        for(let query of queries){
            let queryResult = await attemptQuery({connL1980, connGE1980, query});
            transactionResults.push(queryResult);
        }
        await commit({connGE1980, connL1980});
        connGE1980.release();
        connL1980.release();
        return transactionResults;
    }
    catch(e){
        await rollback({connGE1980, connL1980});
        connGE1980.release();
        connL1980.release();
        throw e;
    }
}

async function dropTempTable({connGE1980, connL1980}){
    try{
        let dropSQL = `DROP TEMPORARY TABLE IF EXISTS ${tempName}`;
        await Promise.all([
            connGE1980.query(dropSQL),
            connL1980.query(dropSQL)
        ]);
    }
    catch(e){
        throw e;
    }
}

async function beginTransactions({connGE1980, connL1980}){
    try{
        await Promise.all([
            connGE1980.beginTransaction(),
            connL1980.beginTransaction()
        ]);
    }
    catch(e){
        throw e;
    }
}

async function executeTransaction({queries, isolationLevel="REPEATABLE READ", readOnly}){
    let { L1980, GE1980 } = promisePools;
    let connL1980 = await L1980.getConnection();
    let connGE1980 = await GE1980.getConnection();
    console.log("Connection established");
    try{
        if(readOnly){
            return await readOnlyTransaction({connGE1980, connL1980, queries, isolationLevel});
        }

        await generateTempTables({connL1980, connGE1980});
        console.log("Temp Tables generated");
        await setIsolationLevel({connL1980, connGE1980, isolationLevel});
        await beginTransactions({connGE1980, connL1980});
        let transactionResults = [];
        for(let query of queries){
            await checkQuery({connL1980, connGE1980, query});
            let queryResult = await attemptQuery({connL1980, connGE1980, query});
            transactionResults.push(queryResult);
        }
        console.log("All Queries executed");
        await commit({connL1980, connGE1980});
        console.log("Committed");
        await dropTempTable({connGE1980, connL1980});
        connL1980.release();
        connGE1980.release();
        requestRebalancing();
        return transactionResults;
    }
    catch(e){
        await rollback({connL1980, connGE1980});
        await dropTempTable({connGE1980, connL1980});
        connL1980.release();
        connGE1980.release();
        console.log("Rollbacked");
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

async function attemptQuery({connGE1980, connL1980, query}){
    try{
        let { type } = parser.astify(query);
        if(type !== "insert"){
            let results = [
                connL1980.execute(query),
                connGE1980.execute(query)
            ];
            results = await Promise.all(results);
            results = results.map((res) => res[0]);
            return results;
        }
        let subTransactions = generateSubTransactions(query);
        let results = []
        if(subTransactions.L1980){
            results.push(connL1980.execute(subTransactions.L1980));
        }
        if(subTransactions.GE1980){
            results.push(connGE1980.execute(subTransactions.GE1980));
        }
        results = await Promise.all(results);
        results = results.map((res) => res[0]);
        return results;
    }
    catch(e){
        throw e;
    }
}

async function checkQuery({connL1980, connGE1980, query}){
    try{
        const originalAst = parser.astify(query);
        if(originalAst.type !== "insert")
            return;
        let tempAst = {...originalAst};
        tempAst.table[0].table = tempName;
        let tempSQL = parser.sqlify(tempAst);
        let promises = [
            connGE1980.execute(tempSQL),
            connL1980.execute(tempSQL)
        ];
        await Promise.all(promises);
    }
    catch(e){
        throw e;
    }
}

async function setIsolationLevel({connGE1980, connL1980, isolationLevel}){
    try{
        let sql = `SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`;
        await Promise.all([
            connGE1980.execute(sql),
            connL1980.execute(sql)
        ]);
    }
    catch(e){
        throw e;
    }
}

async function generateTempTables({connGE1980, connL1980}){
    try{
        let createSQL = `CREATE TEMPORARY TABLE ${tempName} LIKE movies;`;
        let insertSQL = "INSERT INTO " + tempName + " (id, `name`, `year`, `rank`, CENTRAL, L1980, GE1980, tombstone) SELECT id, `name`, `year`, `rank`, CENTRAL, L1980, GE1980, tombstone FROM movies;"
        let promises = [
            connGE1980.execute(createSQL),
            connL1980.execute(createSQL)
        ];
        await Promise.all(promises);
        promises = [
            connL1980.execute(insertSQL),
            connGE1980.execute(insertSQL)
        ];
        await Promise.all(promises);
    }
    catch(e){
        throw e;
    }
}

module.exports = { executeTransaction }