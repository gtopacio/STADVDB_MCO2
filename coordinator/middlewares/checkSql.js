const { Parser } = require('node-sql-parser');
const parser = new Parser();

async function checkSql (req, res, next) {
  let queries = req.body.queries;
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

module.exports = checkSql;
