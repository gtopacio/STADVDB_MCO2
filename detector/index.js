/*
    Detector detects changes in the database then passes the change
    to the Kafka Cluster.
*/

const ZongJi = require('@vlasky/zongji');
const kafkaProducer = require('../lib/kafka/producer');
const formulateChange = require('../lib/formulateChange');

require('dotenv').config();
const changeTopicName = process.env.CHANGE_TOPIC_NAME;

let zongji = new ZongJi({ 
    user: process.env.REPLICATOR_USERNAME,
    password: process.env.REPLICATOR_PASSWORD,
    host: process.env.NODE_HOSTNAME
 });

zongji.on('binlog', async function(evt) {
    let changeData = formulateChange({evt});
    console.log("Bin Log", changeData);
    kafkaProducer.publishChange({topic: changeTopicName, value: JSON.stringify(changeData)});
});

function start(){
    try{
        zongji.start({
            includeEvents: ['tablemap','writerows', 'updaterows', 'deleterows'],
            startAtEnd: true
        });
    }
    catch(e){
        console.error(e);
    }
}

function stop(){
    zongji.stop();
}

module.exports = { start, stop }