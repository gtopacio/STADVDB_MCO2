const { Kafka } = require('kafkajs');
require('dotenv').config();
const kafka = new Kafka({
    clientId: process.env.NODE_NAME,
    brokers: [`${process.env.KAFKA_BROKER_1}:9092`]
});

module.exports = kafka;