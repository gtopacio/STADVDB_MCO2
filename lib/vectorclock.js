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
    let clock = await localDb.executeTransaction({queries: ["SELECT MAX(CENTRAL), MAX(L1980), MAX(GE1980) FROM movies"]});
    console.log(clock);
    serverClock.clock = clock;
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
    merge,
    instantiate
}