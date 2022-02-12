const async = require('hbs/lib/async');
const { Kafka } = require('kafkajs');
require('dotenv').config();

const kafka = new Kafka({
    clientId: process.env.NODE_NAME,
    brokers: [`${process.env.KAFKA_BROKER_1}:9092`],
    retry:{
        restartOnFailure: async function (e){
            console.error(e);
            return true;
        }
    }
});

let consumer = kafka.consumer({ groupId: "test-id" });

async function stop(){
    await consumer.disconnect();
    process.exit(0);
}

async function start(){
    await consumer.connect();
    await consumer.subscribe({ topic: process.env.CENTRAL_CHANGE_TOPIC_NAME, fromBeginning: true });
    await consumer.subscribe({ topic: process.env.L1980_CHANGE_TOPIC_NAME, fromBeginning: true });
    await consumer.subscribe({ topic: process.env.GE1980_CHANGE_TOPIC_NAME, fromBeginning: true });
    await consumer.run({
        eachMessage: async({ topic, message }) => {
            console.log({topic, value: JSON.parse(message.value)});
        }
    });
}

start();
process.on("SIGINT", stop);