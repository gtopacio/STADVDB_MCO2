const axios = require('axios');
const vclock = require('../../lib/vectorclock');
require('dotenv').config();
const CENTRAL_HOSTNAME = process.env.CENTRAL_HOSTNAME;
const COORDINATOR_PORT = process.env.COORDINATOR_PORT;

async function redirectedCheckCentral (req, res, next) {
  console.log("REDIRECTED CHECK CENTRAL IN");
  vclock.increment();
  vclock.merge(req.body.clock);
  next();
}

module.exports = redirectedCheckCentral;
