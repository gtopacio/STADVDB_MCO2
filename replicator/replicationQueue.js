let queue = [];
let unterminated = true;

function add(evt){
    console.log(`Add: ${evt.getName()}`);
    if(unterminated){
        queue.push(evt);
    }
}

function terminate(){
    unterminated = false;
}

module.exports = {
    add, terminate
}

async function main(){
    while(unterminated){
        if(queue.length > 0){
            let evt = queue[0];
            let eventName = evt.getName();
            if(eventName === "writerows"){
                await db.insertStatement(evt);
                continue;
            }
            if(eventName === "deleterows"){
                await db.deleteStatement(evt);
                continue;
            }
            if(eventName === "updaterows"){
                await db.updateStatement(evt);
                continue;
            }
            queue.slice();
        }
    }
}