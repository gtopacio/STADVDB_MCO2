const { Parser } = require('node-sql-parser');
const parser = new Parser();

async function checkSql (req, res, next) {

  let trans = req.body.trans;
  let queries;
  if (trans == "single") {
    queries = req.body.queries;
    console.log(queries);

    for (let i = 0; i < queries.length; i++) {
      try {
        const ast = parser.astify(queries[i]);
      } catch(e) {
        console.error(e);
        res.send("INVALID SQL");
        return;
      }
    }
    next();
  }
  else {
    queryCollection = req.body.queryCollection;
    console.log(queryCollection);

    for (let i = 0; i < queryCollection.length; i++) {
      queries = queryCollection[i];
      for (let j = 0; j < queries.length; j++) {
        try {
          const ast = parser.astify(queries[j]);
        } catch(e) {
          console.error(e);
          res.send("INVALID SQL");
          return;
        }
      }
    }
    next();
  }
}

module.exports = checkSql;
