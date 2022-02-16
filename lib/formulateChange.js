require('dotenv').config();
const origin = process.env.NODE_NAME;

function formulateChange({evt}){
    let { rows } = evt;
    let eventName = evt.getEventName();
    let tableName = "";

    if(eventName !== 'tablemap'){
        let key = Object.keys(evt.tableMap)[0];
        tableName = evt.tableMap[key].tableName;
        if(tableName !== "movies")
            return;
    }

    if(eventName === "writerows"){
        return { tableName, rows, origin, type: "insert"};
    }
    if(eventName === "updaterows"){

        rows = rows.filter((row) => !row.before.tombstone || !row.after.tombstone);

        return { tableName, rows, origin, type: "update"};
    }
}

module.exports = formulateChange;