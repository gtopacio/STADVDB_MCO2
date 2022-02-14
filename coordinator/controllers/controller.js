let db = require('../../lib/database/localDatabase');;
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




    // NODE SQL PARSER
    // FRONT END WITH TABS
    // ONE TAB ONE INPUT ONLY
    // SECOND TAB DYNAMIC INPUT ONE SUBMIT BUTTON
    // PER TEXT AREA CHOOSE NODE TO SEND

    // CHECK ROUTING


    try{
      // req.body.queries.push("DO SLEEP(1)");
      let results = await db.executeTransaction(req.body);
      console.log(results);
      return res.send(results);
    }
    catch(e){
      console.error(e);
      return res.send({e});
    }

    //const { queries } = queries1;
    //let results = await db.executeTransaction(queries);
    //res.send(results);
  },

  ping: async function(req, res){

    try{
      await db.ping();
      res.send(true);
    }
    catch(e){
      res.send(false);
    }




    // try {
    //   let dbPing = await db.ping();
    //   console.log("[controller(" + process.env.NODE_NAME + ")] ping");
    //   res.send(dbPing);
    // } catch(e) {
    //   console.error(e);
    //   console.log("[controller(" + process.env.NODE_NAME + ")] MYSQL down");
    //   if (process.env.NODE_NAME == "CENTRAL") {
    //     let l1980PingURL = "http://" + process.env.L1980_HOSTNAME + ":" + process.env.COORDINATOR_PORT + "/ping"
    //     let ping = await axios.get(l1980PingURL);
    //     if (ping.data) {
    //       console.log("[controller(" + process.env.NODE_NAME + ")] Redirecting to L1980 ping");
    //       res.redirect(307, l1980PingURL);
    //     }
    //   }
    // }
  },

  urls: function(req, res){

    const CENTRAL_HOSTNAME = process.env.CENTRAL_HOSTNAME;
    const L1980_HOSTNAME = process.env.L1980_HOSTNAME;
    const GE1980_HOSTNAME = process.env.GE1980_HOSTNAME;
    const COORDINATOR_PORT = process.env.COORDINATOR_PORT;
    const CENTRAL = `http://${CENTRAL_HOSTNAME}:${COORDINATOR_PORT}/query`;
    const L1980 = `http://${L1980_HOSTNAME}:${COORDINATOR_PORT}/query`;
    const GE1980 = `http://${GE1980_HOSTNAME}:${COORDINATOR_PORT}/query`;

    res.send({ L1980, GE1980, CENTRAL });
  },

  rebalance: async function(req, res){
    if(NODE_NAME === "CENTRAL")
      return res.end();

    let connection = await db.getConnection();

    await connection.beginTransaction();
    let [storedRecord] = await connection.execute("SELECT CENTRAL, GE1980, L1980, tombstone FROM movies WHERE id = ?", [req.body.row.id]);
    storedRecord = storedRecord[0];
    res.send(storedRecord);
    await connection.commit();
    connection.release();
    return;
      
  }
};

module.exports = controller;
