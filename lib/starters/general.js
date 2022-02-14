const coordinator = require('../../coordinator/index');
const detector = require('../../detector/index');
const replicator = require('../../replicator/index');
require('dotenv').config();
let handler;
let topics;

const NODE_NAME = process.env.NODE_NAME;
console.log(`STARTING ${NODE_NAME}`);

if(NODE_NAME === "CENTRAL"){
    handler = require('../handler/centralHandler');
    topics = [process.env.L1980_CHANGE_TOPIC_NAME, process.env.GE1980_CHANGE_TOPIC_NAME];
}
else if(NODE_NAME === "L1980"){
    handler = require('../handler/l1980Handler');
    topics = [process.env.CENTRAL_CHANGE_TOPIC_NAME];
}
else if(NODE_NAME === "GE1980"){
    handler = require('../handler/ge1980Handler');
    topics = [process.env.CENTRAL_CHANGE_TOPIC_NAME];
}

function stopProcess(){
    coordinator.stop();
    replicator.stop();
    detector.stop();
    process.exit(0);
}

async function main(){
    await replicator.start();
    for(let topic of topics){
        await replicator.subscribe(topic);
    }
    await replicator.run(async({message}) => {
        let parsedMessage = JSON.parse(message.value);

        await handler(parsedMessage);
    });
    detector.start();
    coordinator.start();
}

main();
process.on("SIGINT", stopProcess);