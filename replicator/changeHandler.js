const centralHandler = require('./handler/centralHandler');
const ge1980Handler = require('./handler/ge1980Handler');
const l1980Handler = require('./handler/l1980Handler');

require('dotenv').config();


async function changeHandler({ topic, partition, message }){
    if(nodeName === "central")
        return centralHandler({topic, message, poolPromise});
    
    if(nodeName === "l1980"){
        return l1980Handler({topic, message, poolPromise});
    }

    if(nodeName === "ge1980"){
        return ge1980Handler({topic, message, poolPromise});
    }
}

// async function insertStatement({rows}){
//     for(let row of rows){
//         let values = [];
//         values.push(row.id);
//         values.push(row.name);
//         values.push(row.year);
//         values.push(row.rank);
//         if(row.year >= 1980){
//             await ge1980.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`) VALUE (?,?,?,?)", values);
//             continue;
//         }
//         await l1980.execute("INSERT INTO `movies` (`id`, `name`, `year`, `rank`) VALUE (?,?,?,?)", values);
//     }
// }

// async function deleteStatement({rows}){
//     for(let row of rows){
//         let values = [];
//         values.push(row.id);
//         if(row.year >= 1980){
//             await ge1980.execute("DELETE FROM `movies` WHERE `id` = ?", values);
//             continue;
//         }
//         await l1980.execute("DELETE FROM `movies` WHERE `id` = ?", values);
//     }
// }

// async function updateStatement(evt){
//     evt.dump();
// }

module.exports = changeHandler;