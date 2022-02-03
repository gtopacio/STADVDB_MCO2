const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: process.env.NODE_NAME,
    brokers: [`${process.env.KAFKA_BROKER_1}:9092`]
});

module.exports = kafka;