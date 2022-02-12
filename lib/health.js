let health = {
    kafkaConsumer: false,
    kafkaProducer: false,
    localDB: false
};

function setConsumer(val){
    health.kafkaConsumer = !!val;
}

function setProducer(val){
    health.kafkaProducer = !!val;
}

function setLocalDB(val){
    health.localDB = !!val;
}

function isHealthy(){
    for(let check of health){
        if(!check)
            return false;
    }
    return true;
}

module.exports = {
    isHealthy,
    setConsumer,
    setProducer,
    setLocalDB
}