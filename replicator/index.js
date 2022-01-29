require('dotenv').config();
const ZongJi = require('@vlasky/zongji');

let zongji = new ZongJi({ 
    user: process.env.REPLICATOR_USERNAME,
    password: process.env.REPLICATOR_PASSWORD,
    host: process.env.CENTRAL_HOSTNAME
 });

zongji.on('binlog', function(evt) {
    let eventName = evt.getEventName();
    console.log(eventName, evt.rows);
});

zongji.start({
  includeEvents: ['tablemap','writerows', 'updaterows', 'deleterows']
});