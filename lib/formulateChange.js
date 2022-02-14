require('dotenv').config();
const origin = process.env.NODE_NAME;

function formulateChange({evt}){
    let { timestamp, rows } = evt;
    let eventName = evt.getEventName();
    let tableName = "";

    if(eventName !== 'tablemap'){
        let key = Object.keys(evt.tableMap)[0];
        tableName = evt.tableMap[key].tableName;
        if(tableName !== "movies")
            return;
    }

    if(eventName === "writerows"){
        return { timestamp, tableName, rows, origin, type: "insert"};
    }
    if(eventName === "updaterows"){
        return { timestamp, tableName, rows, origin, type: "update"};
    }
}

module.exports = formulateChange;