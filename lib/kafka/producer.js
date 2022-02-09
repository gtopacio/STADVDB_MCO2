const kafka = require('./connection');

async function publishChange({topic, value}){
    const producer = kafka.producer();
    try{
        await producer.connect();
        await producer.send({
            topic,
            messages: [{value}]
        });
        console.log("Kafka Send", {value});
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