const handler = require('./lib/handler/centralHandler');

const lastUpdated = new Date("2-2-2020").toString();

const feb2020 = new Date("2-2-2020");
const feb2021 = new Date("2-2-2021");
const date1970 = new Date("2-2-1970");

let update1 = {
    origin: "CENTRAL",
    timestamp: 1644396489,
    type: "update",
    rows: [ {
        before: { id: 412332, name: 'TESTING 1', year: 2000, rank: 9, lastUpdated },
        after: { id: 412332, name: 'TESTING 1', year: 2020, rank: 2, lastUpdated }
    } ]
}

let insert1 = {
    origin: "CENTRAL",
    timestamp: 1444396489,
    type: "insert",
    rows: [{ id: 412332, name: 'TESTING 1', year: 2000, rank: 9, lastUpdated }]
}

let delete1 = {
    origin: "CENTRAL",
    timestamp: feb2021.getTime(),
    type: "delete",
    rows: [
        { id: 412332, name: 'TESTING 1', year: 2000, rank: null, lastUpdated }
    ]
}

handler(update1).then(() => {
    process.exit(0);
});

