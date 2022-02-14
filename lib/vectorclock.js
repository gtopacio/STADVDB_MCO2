const localDb = require('../lib/database/localDatabase');
const vectorclock = require('vectorclock');
require('dotenv').config();
const NODE_NAME = process.env.NODE_NAME;

let serverClock = { clock: {} };

async function instantiate(){
    let clocks = await localDb.executeTransaction({
        queries: ["SELECT MAX(CENTRAL) AS CENTRAL, MAX(L1980) AS L1980, MAX(GE1980) AS GE1980 FROM movies"]
    });
    serverClock.clock = clocks;
}

function increment(){
    vectorclock.increment(serverClock, NODE_NAME);
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
    merge,
    instantiate
}