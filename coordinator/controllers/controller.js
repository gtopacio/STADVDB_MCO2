let db = require('../../lib/database/localDatabase');;
const axios = require('axios');
require('dotenv').config();

const NODE_NAME = process.env.NODE_NAME;
if(NODE_NAME !== "CENTRAL"){
  db = require('../../lib/database/partitionedDatabase');
}

const controller = {
  getIndex: function (req, res) {
    res.render('index');
  },

  query: async function (req, res) {
    // var query1 = req.body.query1;
    // // var query2 = req.body.query2;

    // var queries1 = query1.split(";");
    // // var queries2 = query2.split(";");

    // for (i = 0; i < queries1.length; i++)
    //    queries1[i] = queries1[i].trim();
    // // for (i = 0; i < queries2.length; i++)
    // //   queries2[i] = queries2[i].trim();

    // console.log("Queries1: ");
    // console.log(queries1);

    // console.log("Queries2: ");
    // console.log(queries2);

    // BACKEND CODE
    try{
      let results = await db.executeTransaction(req.body);
      console.log(results);
      res.send(results);
    }
    catch(e){
      console.error(e);
      res.send({e});
    }

    //const { queries } = queries1;
    //let results = await db.executeTransaction(queries);
    //res.send(results);
  },

  ping: async function(req, res){
    try {
      let dbPing = await db.ping();
      console.log("[controller(" + process.env.NODE_NAME + ")] ping");
      res.send(dbPing);
    } catch(e) {
      console.error(e);
      console.log("[controller(" + process.env.NODE_NAME + ")] MYSQL down");
      if (process.env.NODE_NAME == "CENTRAL") {
        let l1980PingURL = "http://" + process.env.L1980_HOSTNAME + ":" + process.env.COORDINATOR_PORT + "/ping"
        let ping = await axios.get(l1980PingURL);
        if (ping.data) {
          console.log("[controller(" + process.env.NODE_NAME + ")] Redirecting to L1980 ping");
          res.redirect(307, l1980PingURL);
        }
      }
    }
  }
};

module.exports = controller;
