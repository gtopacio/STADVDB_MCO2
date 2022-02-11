const coordinator = require('../../coordinator/index');
const detector = require('../../detector/index');
const replicator = require('../../replicator/index');
require('dotenv').config();
const handler = require('../handler/l1980Handler');
const topics = [process.env.CENTRAL_CHANGE_TOPIC_NAME];

function stopProcess(){
    coordinator.stop();
    replicator.stop();
    detector.stop();
    process.exit(0);
}

async function main(){
    await replicator.start();
    for(let topic of topics){
        await replicator.subscribe(topic, async({topic, partition, message}) => {
            let parsedMessage = JSON.parse(message.value);
            await handler(parsedMessage);
        });
    }
    detector.start();
    coordinator.start();
}

main();
process.on("SIGINT", stopProcess);