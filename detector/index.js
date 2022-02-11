/*
    Detector detects changes in the database then passes the change
    to the Kafka Cluster.
*/

const ZongJi = require('@vlasky/zongji');
// const kafkaProducer = require('../lib/kafka/producer');
const formulateChange = require('../lib/formulateChange');

// const handler = require('../lib/handler/centralHandler');

require('dotenv').config();
const changeTopicName = process.env.CHANGE_TOPIC_NAME;
let timeOut;

let zongji;

function binlogHandler(evt) {
    let changeData = formulateChange({evt});
    if(!changeData)
        return;
    console.log(changeData);
    // console.log("Bin Log", stringified);
    // console.log(parsed);
    // console.log(new Date(parsed.rows[0].lastUpdated));
    // handler(parsed);
    // kafkaProducer.publishChange({topic: changeTopicName, value: JSON.stringify(changeData)});
}

function retryConnection(e){
    if(e.code === "PROTOCOL_CONNECTION_LOST" || e.code === "ECONNREFUSED"){
        console.log({message: "Detector Error, restarting connection...", code: e.code});
        if(zongji)
            zongji.stop();
        timeOut = setTimeout(start, 1000);
    }
}

function start(){
    if(timeOut){
        clearTimeout(timeOut);
    }
    console.log("Detector is starting...");
    let zongji = new ZongJi({ 
        user: process.env.REPLICATOR_USERNAME,
        password: process.env.REPLICATOR_PASSWORD,
        host: process.env.NODE_HOSTNAME,
        port: process.env.MYSQL_PORT,
        serverId: 98
     });
    zongji.on('binlog', binlogHandler);
    zongji.on('error', retryConnection);
    zongji.on('ready', () => console.log("Detector started"));
    zongji.start({
        includeEvents: ['tablemap','writerows', 'updaterows', 'deleterows'],
        startAtEnd: true
    });
}

function stop(){
    clearTimeout(timeOut);
    if(zongji){
        zongji.stop();
    }
}

module.exports = { start, stop }