const axios = require('axios');
const vclock = require('../../lib/vectorclock');
require('dotenv').config();
const CENTRAL_HOSTNAME = process.env.CENTRAL_HOSTNAME;
const COORDINATOR_PORT = process.env.COORDINATOR_PORT;
const centralQueryURL = `http://${CENTRAL_HOSTNAME}:${COORDINATOR_PORT}/query/redirected`;

const centralPingURL = `http://${CENTRAL_HOSTNAME}:${COORDINATOR_PORT}/ping`;

async function checkCentral (req, res, next) {

  console.log("CHECK CENTRAL IN");

  let clock = vclock.increment();
    req.body.clock = clock;
    console.log(req.body.clock);

  if (process.env.NODE_NAME == "CENTRAL") {
    next();
    return;
  }

  try{
    let { data } = await axios.get(centralPingURL);
    if(data){
      return await axios.post(centralQueryURL, req.body);
    }
    next();
  }
  catch(e){
    next();
  }
}

module.exports = checkCentral;
