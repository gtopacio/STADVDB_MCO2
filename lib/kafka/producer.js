const kafka = require('./connection');

async function publishChange({topic, value}){
    try{
        const producer = kafka.producer();
        await producer.connect();
        await producer.send({
            topic,
            messages: [{value}]
        });
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