/*
    Detector detects changes in the database then passes the change
    to the Kafka Cluster.
*/

const ZongJi = require('@vlasky/zongji');
const kafkaProducer = require('../lib/kafka/producer');
const formulateChange = require('../lib/formulateChange');

require('dotenv').config();
const origin = process.env.NODE_NAME;
const centralTopic = process.env.CENTRAL_CHANGE_TOPIC_NAME;
const l1980Topic = process.env.L1980_CHANGE_TOPIC_NAME;
const ge1980Topic = process.env.GE1980_CHANGE_TOPIC_NAME;

let zongji = new ZongJi({ 
    user: process.env.REPLICATOR_USERNAME,
    password: process.env.REPLICATOR_PASSWORD,
    host: process.env.NODE_HOSTNAME
 });

zongji.on('binlog', async function(evt) {
    let changeData = formulateChange({evt, origin});
    if(origin === "central"){
        kafkaProducer.publishChange({topic: centralTopic, value: JSON.stringify(changeData)});
        return;
    }
    if(origin === "l1980"){
        kafkaProducer.publishChange({topic: l1980Topic, value: JSON.stringify(changeData)});
        return;
    }
    if(origin === "ge1980"){
        kafkaProducer.publishChange({topic: ge1980Topic, value: JSON.stringify(changeData)});
        return;
    }
});

function start(){
    zongji.start({
        includeEvents: ['tablemap','writerows', 'updaterows', 'deleterows']
      });
}

function stop(){
    zongji.stop();
    queue.terminate();
}

module.exports = { start, stop }