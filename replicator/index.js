const ZongJi = require('@vlasky/zongji');
const db = require('./db');
require('dotenv').config();

let zongji = new ZongJi({ 
    user: process.env.REPLICATOR_USERNAME,
    password: process.env.REPLICATOR_PASSWORD,
    host: process.env.CENTRAL_HOSTNAME
 });

zongji.on('binlog', async function(evt) {
    let eventName = evt.getEventName();
    if(eventName === "writerows"){
        await db.insertStatement(evt);
        return;
    }
    if(eventName === "deleterows"){
        await db.deleteStatement(evt);
        return;
    }
    if(eventName === "updaterows"){
        await db.updateStatement(evt);
    }
});

zongji.start({
  includeEvents: ['tablemap','writerows', 'updaterows', 'deleterows']
});

process.on('SIGINT', () => {
  zongji.stop();
  queue.terminate();
})