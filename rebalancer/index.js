const { default: axios } = require('axios');
const kafka = require('../lib/kafka/connection');
require('dotenv').config();
const consumer = kafka.consumer({"groupId": "rebalancer"});
const topics = [process.env.L1980_CHANGE_TOPIC_NAME, process.env.GE1980_CHANGE_TOPIC_NAME];

const rebalanceL1980 = `http://${process.env.L1980_HOSTNAME}:${process.env.COORDINATOR_PORT}/rebalance`;
const rebalanceGE1980 = `http://${process.env.GE1980_HOSTNAME}:${process.env.COORDINATOR_PORT}/rebalance`;

async function start(){
    await consumer.connect();
    for(let topic of topics){
        await consumer.subscribe({topic});
    }
    await consumer.run({
        "eachMessage": async function({message}){
            let {type, rows, origin} = JSON.parse(message.value);
            for(let row of rows){
                
                if(origin === "L1980"){
                    if(type === "update" && row.after.year >= 1980 && row.after.tombstone == 0){
                        axios.post(rebalanceL1980, {row: row.after});
                        axios.post(rebalanceGE1980, {row: row.after});
                        continue;
                    }

                    if(type === "insert" && row.year >= 1980 && row.tombstone == 0){
                        axios.post(rebalanceL1980, {row: row.after});
                        axios.post(rebalanceGE1980, {row: row.after});
                        continue;
                    }
                }
                
                if(type === "update" && row.after.year < 1980 && row.after.tombstone == 0){
                    axios.post(rebalanceL1980, {row: row.after});
                    axios.post(rebalanceGE1980, {row: row.after});
                    continue;
                }

                if(type === "insert" && row.year < 1980 && row.tombstone == 0){
                    axios.post(rebalanceL1980, {row: row.after});
                    axios.post(rebalanceGE1980, {row: row.after});
                    continue;
                }

            }
        }
    });
}

function stop(){
    consumer.disconnect();
}

start();

process.on("SIGINT", stop);