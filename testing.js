const handler = require('./lib/handler/centralHandler');

let insert1 = {
    origin: "CENTRAL",
    timestamp: 1644396489,
    type: "update",
    rows: [ {
        before: { id: 412330, name: 'TESTING 1', year: 2000, rank: null, lastUpdated: 1644396481 },
        after: { id: 412330, name: 'TESTING 1', year: 2020, rank: 9, lastUpdated: 1644396489 }
    } ]
}

handler(insert1).then(() => {
    process.exit(0);
});

