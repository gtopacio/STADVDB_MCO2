function formulateChange({evt, origin}){
    let { timestamp, rows } = evt;
    let eventName = evt.getEventName();
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