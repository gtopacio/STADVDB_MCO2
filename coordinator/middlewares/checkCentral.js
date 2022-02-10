const axios = require('axios');
const db = require('../../lib/database/localDatabase');
require('dotenv').config();

async function checkCentral (req, res, next) {
  if (process.env.NODE_NAME == "CENTRAL") {
    console.log("[checkCentral] This is CENTRAL");
    next();
  }
  else {
    try {
      let centralPingURL = "http://" + process.env.CENTRAL_HOSTNAME + ":" + process.env.COORDINATOR_PORT + "/query"
      let ping = await axios.get(centralPingURL);
      if (ping.data) {
        console.log("[checkCentral(" + process.env.NODE_NAME + ")] Redirecting to CENTRAL Ping");
        res.redirect(307, centralPingURL);
      }
    } catch(e) {
      console.log("[checkCentral(" + process.env.NODE_NAME + ")] CENTRAL down");
      next();
    }
  }
}

module.exports = checkCentral;
