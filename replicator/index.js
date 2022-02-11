/*
    Replicator subscribes to the Kafka Cluster for changes then
    applies it to the database.
*/

const kafka = require('../lib/kafka/connection');

require('dotenv').config();
const consumer = kafka.consumer({ groupId: process.env.NODE_NAME });

async function start(){
    await consumer.connect();
}

async function subscribe(topic){
    try{
        await consumer.subscribe({topic, fromBeginning: true});
    }
    catch(e){
        console.error(e);
    }
}

async function run(eachMessage){
    await consumer.run({eachMessage});
}

async function stop(){
    await consumer.disconnect();
}

module.exports = { start, stop, subscribe, run }


