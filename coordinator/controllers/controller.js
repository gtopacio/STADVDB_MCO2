const controller = {
  getIndex: function (req, res) {
    res.render('index');
  },

  query: function (req, res) {
    var query1 = req.body.query1;
    var query2 = req.body.query2;

    var queries1 = query1.split(",");
    var queries2 = query2.split(",");

    for (i = 0; i < queries1.length; i++)
      queries1[i] = queries1[i].trim();
    for (i = 0; i < queries2.length; i++)
      queries2[i] = queries2[i].trim();

    console.log("Queries1: ");
    console.log(queries1);

    console.log("Queries2: ");
    console.log(queries2);

    //const { queries } = queries1;
    //let results = await db.executeTransaction(queries);
    //res.send(results);
  }
};

module.exports = controller;
