require('dotenv').config();
function generateDatabaseDetails(nodeName){
    let details = {
        user: process.env.COORDINATOR_USERNAME,
        password: process.env.COORDINATOR_PASSWORD,
        database: process.env.DATABASE_NAME
    }

    if(nodeName === "central"){
        details.host = process.env.CENTRAL_HOSTNAME;
        return details;
    }

    if(nodeName === "l1980"){
        details.host = process.env.L1980_HOSTNAME;
        return details;
    }

    if(nodeName === "central"){
        details.host = process.env.GE1980_HOSTNAME;
        return details;
    }
}

module.exports = generateDatabaseDetails;