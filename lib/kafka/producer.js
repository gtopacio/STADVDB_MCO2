const kafka = require('./connection');
const { CompressionTypes } = require('kafkajs');

async function publishChange({topic, value}){
    const producer = kafka.producer({
        idempotent: true,
        maxInFlightRequests: 1
    });
    try{
        
        await producer.connect();
        
        await producer.send({
            topic,
            messages: [{value, partition: 0}],
            compression: CompressionTypes.GZIP
        });
        try{
            let parsed = JSON.parse(value);
            console.log("Kafka Send", parsed);
        }
        catch(e){
            console.log("Kafka Send", value);
        }
    }
    catch(e){
        throw e;
    }
    finally{
        await producer.disconnect();
    }
}

module.exports = {
    publishChange
}