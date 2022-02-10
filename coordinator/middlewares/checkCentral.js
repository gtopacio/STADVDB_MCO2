const axios = require('axios');
const db = require('../../lib/database/localDatabase');
require('dotenv').config();

async function checkCentral (req, res, next) {
  if (process.env.NODE_NAME == "CENTRAL") {
    console.log("THIS IS CENTRAL");
    next();
  }
  else {
    //console.log("CENTRAL: " + (await axios.get("http://" + process.env.CENTRAL_HOSTNAME + ":" + process.env.COORDINATOR_PORT + "/ping")));
    //return;
    let ping;
    try {
      ping = await axios.get("http://" + process.env.CENTRAL_HOSTNAME + ":" + process.env.COORDINATOR_PORT + "/ping");
      if (ping.data) {
        console.log("REDIRECT");
        console.log("CENTRAL PING: " + await db.ping());
        res.redirect(307, "http://" + process.env.CENTRAL_HOSTNAME + ":" + process.env.COORDINATOR_PORT + "/ping");
      }
    } catch(e) {
      console.log("CENTRAL DOWN");
      console.log("L1980 PING: " + await db.ping());
      next();
    }
  }
}

module.exports = checkCentral;
