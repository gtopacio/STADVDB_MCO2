/*
    Replicator subscribes to the Kafka Cluster for changes then
    applies it to the database.
*/

const kafka = require('../lib/kafka/connection');

require('dotenv').config();
const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID });

async function start(topic, eachMessage){
    await consumer.connect();
    await consumer.subscribe({topic});
    await consumer.run({eachMessage});
}

async function stop(){
    await consumer.disconnect();
}

module.exports = { start, stop }


