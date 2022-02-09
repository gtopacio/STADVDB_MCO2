require('dotenv').config();
const origin = process.env.NODE_NAME;

function formulateChange({evt}){
    let { timestamp, rows } = evt;
    let eventName = evt.getEventName();

    if(eventName !== 'tablemap'){
        rows = rows.map((row) => {
            row.lastUpdated = row.lastUpdated.getTime() * 0.001;
            return row;
        });
    }

    if(eventName === "writerows"){
        return { timestamp, rows, origin, type: "insert"};
    }
    if(eventName === "deleterows"){
        return { timestamp, rows, origin, type: "delete"};
    }
    if(eventName === "updaterows"){
        return { timestamp, rows, origin, type: "update"};
    }
}

module.exports = formulateChange;