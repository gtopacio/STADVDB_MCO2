const axios = require('axios');
const db = require('../../lib/database/localDatabase');
require('dotenv').config();
const CENTRAL_HOSTNAME = process.env.CENTRAL_HOSTNAME;
const L1980_HOSTNAME = process.env.L1980_HOSTNAME;
const GE1980_HOSTNAME = process.env.GE1980_HOSTNAME;
const COORDINATOR_PORT = process.env.COORDINATOR_PORT;
const centralQueryURL = `http://${CENTRAL_HOSTNAME}:${COORDINATOR_PORT}/query`;
const l1980QueryURL = `http://${L1980_HOSTNAME}:${COORDINATOR_PORT}/query`;
const ge1980QueryURL = `http://${GE1980_HOSTNAME}:${COORDINATOR_PORT}/query`;

const centralPingURL = `http://${CENTRAL_HOSTNAME}:${COORDINATOR_PORT}/ping`;
const l1980PingURL = `http://${L1980_HOSTNAME}:${COORDINATOR_PORT}/ping`;
const ge1980PingURL = `http://${GE1980_HOSTNAME}:${COORDINATOR_PORT}/ping`;

// let axiosInstance = axios.create({
//   timeout: 5000
// });

async function checkCentral (req, res, next) {

  console.log("CHECK CENTRAL IN");

  if (process.env.NODE_NAME == "CENTRAL") {
    next();
    return;
  }

  try{
    let { data } = await axios.get(centralPingURL);
    if(data){
      res.redirect(307, centralQueryURL);
      return;
    }
    next();
  }
  catch(e){
    next();
  }
}

module.exports = checkCentral;
