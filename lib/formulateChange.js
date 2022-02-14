require('dotenv').config();
const origin = process.env.NODE_NAME;

function formulateChange({evt}){
    let { timestamp, rows } = evt;
    let tableName = "";
    let eventName = evt.getEventName();

    if(eventName !== 'tablemap'){
        rows = rows.map((row) => {
            row.lastUpdated = row.lastUpdated;
            return row;
        });
        let key = Object.keys(evt.tableMap)[0];
        tableName = evt.tableMap[key].tableName;
    }

    if(eventName === "writerows"){
        return { timestamp, tableName, rows, origin, type: "insert"};
    }
    if(eventName === "deleterows"){
        return { timestamp, tableName, rows, origin, type: "delete"};
    }
    if(eventName === "updaterows"){
        return { timestamp, tableName, rows, origin, type: "update"};
    }
}

module.exports = formulateChange;