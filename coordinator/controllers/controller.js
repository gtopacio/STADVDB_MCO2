const db = require('../../lib/database/localDatabase');
const axios = require('axios');
require('dotenv').config();

const controller = {
  getIndex: function (req, res) {
    res.render('index');
  },

  query: async function (req, res) {
    // var query1 = req.body.query1;
    // var query2 = req.body.query2;

    // var queries1 = query1.split(";");
    // var queries2 = query2.split(";");

    // for (i = 0; i < queries1.length; i++)
    //   queries1[i] = queries1[i].trim();
    // for (i = 0; i < queries2.length; i++)
    //   queries2[i] = queries2[i].trim();

    // console.log("Queries1: ");
    // console.log(queries1);

    // console.log("Queries2: ");
    // console.log(queries2);

    // BACKEND CODE
    try{
      //let results = await db.executeTransaction(req.body);
      res.send(req.body);
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
      let ping = await db.ping();
      let msg = process.env.NODE_NAME + " ping: " + ping;
      console.log(msg);
      res.send(msg)
    } catch(e) {
      console.error(e);
      console.log(process.env.NODE_NAME + " MYSQL DOWN");
      res.send(msg);
    }
  }
};

module.exports = controller;
