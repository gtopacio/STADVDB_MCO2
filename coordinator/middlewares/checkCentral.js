const axios = require('axios');
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
        res.redirect(307, "http://" + process.env.CENTRAL_HOSTNAME + ":" + process.env.COORDINATOR_PORT + "/ping");
      }
    } catch(e) {
      console.log("CENTRAL DOWN");
      console.log("NEXT");
      next();
    }
  }
}

module.exports = checkCentral;
