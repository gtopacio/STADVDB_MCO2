const kafka = require('./connection');
const { CompressionTypes } = require('kafkajs');



async function publishChange({topic, value}){
    try{
        const producer = kafka.producer({
            idempotent: true,
            maxInFlightRequests: 1
        });
        
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
}

async function stop(){
    await producer.disconnect();
}

module.exports = {
    publishChange, stop
}