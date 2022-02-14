const vectorclock = require('vectorclock');
const localDb = require('./database/localDatabase');
require('dotenv').config();
const NODE_NAME = process.env.NODE_NAME;

let serverClock = { clock: {
    CENTRAL: 0,
    L1980: 0,
    GE1980: 0
} };

async function instantiate(){
    let sql = "SELECT MAX(CENTRAL) AS CENTRAL, MAX(L1980) AS L1980, MAX(GE1980) AS GE1980 FROM movies";
    let [res] = await localDb.executeTransaction({
        queries: [sql]
    });
    console.log(res);
    serverClock.clock = res[0];
}

function increment(){
    return vectorclock.increment(serverClock, NODE_NAME);
}

function get(){
    return {...serverClock};
}

function merge(clock){
    serverClock = vectorclock.merge(serverClock, clock);
    return {...serverClock};
}

module.exports = {
    increment,
    get,
    merge
}