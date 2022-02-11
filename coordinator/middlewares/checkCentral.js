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

async function checkCentral (req, res, next) {
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
    try{
      let { data } = await axios.get(l1980PingURL);
      if(data){
        res.redirect(307, l1980QueryURL);
        return;
      }

      res.send({
        e: "Two nodes are down"
      });
    }
    catch(e){
      res.send({
        e: "Two nodes are down"
      });
    }
  }
  catch(e){
    try{
      let { data } = await axios.get(l1980PingURL);
      if(data){
        res.redirect(307, l1980QueryURL);
        return;
      }

      res.send({
        e: "Two nodes are down"
      });
    }
    catch(e){
      res.send({
        e: "Two nodes are down"
      });
    }
  }
}

module.exports = checkCentral;
