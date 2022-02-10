const axios = require('axios');
require('dotenv').config();

async function checkCentral (req, res, next) {
  if (process.env.NODE_NAME == "CENTRAL") {
    console.log("THIS IS CENTRAL");
    next();
  }
  else {
    console.log("CENTRAL: " + (await axios.get("http://" + process.env.CENTRAL_HOSTNAME + ":" + process.env.COORDINATOR_PORT + "/ping")));
    return;
    /*
    if (await axios.get("http://" + process.env.CENTRAL_HOSTNAME + ":" + process.env.COORDINATOR_PORT + "/ping")) {
      res.redirect("http://" + process.env.CENTRAL_HOSTNAME + ":" + process.env.COORDINATOR_PORT + "/ping", 307);
    }
    else {
      next();
    }
    */
  }
}

module.exports = checkCentral;
