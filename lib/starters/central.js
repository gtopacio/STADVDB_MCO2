const coordinator = require('../../coordinator/index');
const detector = require('../../detector/index');
const replicator = require('../../replicator/index');
require('dotenv').config();
const handler = require('../handler/centralHandler');
const topics = [process.env.L1980_CHANGE_TOPIC_NAME, process.env.GE1980_CHANGE_TOPIC_NAME];

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
    await replicator.run(async({message, heartbeat}) => {
        let parsedMessage = JSON.parse(message.value);
        await handler(parsedMessage, heartbeat);
    });
    detector.start();
    coordinator.start();
}

main();
process.on("SIGINT", stopProcess);