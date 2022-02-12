const { Parser } = require('node-sql-parser');
const parser = new Parser();

async function checkLevel (req, res, next) {

  let trans = req.body.trans;

  if (trans == "single") {
    queries = req.body.queries;

    for (let i = 0; i < queries.length; i++) {
      const ast = parser.astify(queries[i]);
      if (ast.type != "select") {
          req.body.readOnly = false;
          console.log("NOT READ ONLY");
          next();
          return;
      }
    }
    console.log("READ ONLY");
    req.body.readOnly = true;
    next();
  }
  else {
    queryCollection = req.body.queryCollection;

    for (let i = 0; i < queryCollection.length; i++) {
      queries = queryCollection[i];
      for (let j = 0; j < queries.length; j++) {
        const ast = parser.astify(queries[j]);
        if (ast.type != "select") {
            req.body.readOnly = false;
            console.log("NOT READ ONLY");
            next();
            return;
        }
      }
    }
    console.log("READ ONLY");
    req.body.readOnly = true;
    next();
  }
}

module.exports = checkLevel;
