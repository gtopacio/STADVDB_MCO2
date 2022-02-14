const { Parser } = require('node-sql-parser');
const parser = new Parser();

async function checkReadOnly (req, res, next) {
  let queries = req.body.queries;
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

module.exports = checkReadOnly;
