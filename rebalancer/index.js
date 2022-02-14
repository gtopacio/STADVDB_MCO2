const mysql = require('mysql2');
const kafka = require('../lib/kafka/connection');
const vectorClock = require('vectorclock');
require('dotenv').config();
const consumer = kafka.consumer({"groupId": "rebalancer", "maxInFlightRequests": 1});
const topics = [process.env.L1980_CHANGE_TOPIC_NAME, process.env.GE1980_CHANGE_TOPIC_NAME];

const user = process.env.BACKEND_DB_USERNAME;
const password = process.env.BACKEND_DB_PASSWORD;
const database = process.env.DATABASE_NAME;

const rebalanceL1980 = `http://${process.env.L1980_HOSTNAME}:${process.env.COORDINATOR_PORT}/rebalance`;
const rebalanceGE1980 = `http://${process.env.GE1980_HOSTNAME}:${process.env.COORDINATOR_PORT}/rebalance`;

let pools = {
    L1980: mysql.createPool({
        host: process.env.L1980_HOSTNAME,
        user,
        password,
        database,
        connectionLimit: 10,
        queueLimit: 0,
        // acquireTimeout: 5000
    }),
    GE1980: mysql.createPool({
        host: process.env.GE1980_HOSTNAME,
        user,
        password,
        database,
        connectionLimit: 10,
        queueLimit: 0,
        // acquireTimeout: 5000
    })
};

let promisePools = {
    L1980: pools.L1980.promise(),
    GE1980: pools.GE1980.promise()
};

async function start(){
    await consumer.connect();
    for(let topic of topics){
        await consumer.subscribe({topic});
    }
    await consumer.run({
        "eachMessage": async function({message}){
            let {type, rows, origin} = JSON.parse(message.value);

            let connL1980 = await promisePools.L1980.getConnection();
            let connGE1980 = await promisePools.GE1980.getConnection();

            await Promise.all([connGE1980.execute("SET sql_log_bin = OFF"), connL1980.execute("SET sql_log_bin = OFF")]);

            for(let row of rows){

                let record = row;

                if(type === "update"){
                    record = row.after;
                }
                
                if(origin === "L1980" && record.year >= 1980 && record.tombstone == 0){
                    console.log("ATTEMPT", record);
                    let receivedClock = {clock: {CENTRAL: record.CENTRAL, L1980: record.L1980, GE1980: record.GE1980}};
                    await Promise.all([connL1980.beginTransaction(), connGE1980.beginTransaction()]);
                    let [storedRecordL1980] = connL1980.execute("SELECT CENTRAL, L1980, GE1980, tombstone FROM movie WHERE id = ? FOR UPDATE", [row.after.id]);
                    storedRecordL1980 = storedRecordL1980[0];
                    recordL1980 = {clock: storedRecordL1980};
                    if(vectorClock.isIdentical(recordL1980, receivedClock)){
                        connL1980.execute("UPDATE SET tombstone=true WHERE id=?", [row.after.id]).then(async() =>{
                            await connL1980.commit();
                            connL1980.release();
                            console.log("L1980 Committed");
                        }).catch(async(e) => {
                            await connL1980.rollback();
                            console.error(e);
                            connL1980.release();
                            console.log("L1980 Rollbacked");
                        });
                    }

                    let [storedRecordGE1980] = await connGE1980.execute("SELECT CENTRAL, L1980, GE1980, tombstone FROM movie WHERE id = ? FOR UPDATE");
                    storedRecordGE1980 = storedRecordGE1980[0];
                    recordGE1980 = {clock: storedRecordGE1980};

                    if(vectorClock.compare(receivedClock, recordGE1980) === vectorClock.GT){
                        try{
                            if(storedRecordGE1980){
                                let values = [record.id, record.name, record.year, record.rank, record.CENTRAL, record.L1980, record.GE1980. record.tombstone];
                                await connGE1980.execute("UPDATE movies SET id = ?, `name` = ?, `year` = ?, `rank` = ?, CENTRAL = ?, L1980 = ?, GE1980 = ?, tombstone = ?", values);
                            }
                            else{
                                let values = [record.id, record.name, record.year, record.rank, record.CENTRAL, record.L1980, record.GE1980. record.tombstone];
                                await connGE1980.execute("INSERT INTO movies (id,`name`,`year`,`rank`,CENTRAL,L1980,GE1980,tomstone) VALUES (?,?,?,?,?,?,?)", values);
                            }
                            await connGE1980.commit();
                            console.log("GE1980 Committed");
                            connGE1980.release();
                        }
                        catch(e){
                            console.error(e);
                            await connGE1980.rollback();
                            connGE1980.release();
                            console.log("GE1980 Rollbacked");
                        }
                    }
                    continue;
                }
                
                if(origin == "GE1980" && record.year < 1980 && record.tombstone == 0){
                    let receivedClock = {clock: {CENTRAL: record.CENTRAL, L1980: record.L1980, GE1980: record.GE1980}};
                    await Promise.all([connL1980.beginTransaction(), connGE1980.beginTransaction()]);
                    let [storedRecordGE1980] = connGE1980.execute("SELECT CENTRAL, L1980, GE1980, tombstone FROM movie WHERE id = ? FOR UPDATE", [row.after.id]);
                    storedRecordGE1980 = storedRecordGE1980[0];
                    recordGE1980 = {clock: storedRecordGE1980};
                    if(vectorClock.isIdentical(recordGE1980, receivedClock)){
                        connGE1980.execute("UPDATE SET tombstone=true WHERE id=?", [row.after.id]).then(async() =>{
                            await connL1980.commit();
                            connGE1980.release();
                            console.log("GE1980 Committed");
                        }).catch(async(e) => {
                            await connGE1980.rollback();
                            console.error(e);
                            connGE1980.release();
                            console.log("GE1980 ROllbacked");
                        });
                    }

                    let [storedRecordL1980] = await connL1980.execute("SELECT CENTRAL, L1980, GE1980, tombstone FROM movie WHERE id = ? FOR UPDATE");
                    storedRecordL1980 = storedRecordL1980[0];
                    recordL1980 = {clock: storedRecordL1980};

                    if(vectorClock.compare(receivedClock, recordL1980) === vectorClock.GT){
                        try{
                            if(storedRecordL1980){
                                let values = [record.id, record.name, record.year, record.rank, record.CENTRAL, record.L1980, record.GE1980. record.tombstone];
                                await connL1980.execute("UPDATE movies SET id = ?, `name` = ?, `year` = ?, `rank` = ?, CENTRAL = ?, L1980 = ?, GE1980 = ?, tombstone = ?", values);
                            }
                            else{
                                let values = [record.id, record.name, record.year, record.rank, record.CENTRAL, record.L1980, record.GE1980. record.tombstone];
                                await connL1980.execute("INSERT INTO movies (id,`name`,`year`,`rank`,CENTRAL,L1980,GE1980,tomstone) VALUES (?,?,?,?,?,?,?)", values);
                            }
                            await connL1980.commit();
                            connL1980.release();
                            console.log("L1980 Committed");
                        }
                        catch(e){
                            console.error(e);
                            await connL1980.rollback();
                            connL1980.release();
                            console.log("L1980 Rollbacked");
                        }
                    }
                    continue;
                }

            }
        }
    });
}

async function stop(){
    await consumer.disconnect();
    process.exit(0);
}

start();

process.on("SIGINT", stop);