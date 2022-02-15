/*
    Replicator subscribes to the Kafka Cluster for changes then
    applies it to the database.
*/

const kafka = require('../lib/kafka/connection');

require('dotenv').config();
let consumer = kafka.consumer({ 
    groupId: process.env.NODE_NAME,
    maxInFlightRequests: 1
});

const { CRASH } = consumer.events;

let remover = consumer.on(CRASH, () => {
    console.log("CONSUMER: Crashed...");
});

async function start(){
    await consumer.connect();
}

async function subscribe(topic){
    try{
        await consumer.subscribe({topic});
        console.log("Subscribed to " + topic);
    }
    catch(e){
        console.error(e);
    }
}

async function run(eachMessage){
    await consumer.run({
        eachMessage,
        autoCommit: true
    });
}

async function stop(){
    await consumer.disconnect();
    remover();
}

module.exports = { start, stop, subscribe, run }


