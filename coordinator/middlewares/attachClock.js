let vclock = require('../../lib/vectorclock');
const { Parser } = require('node-sql-parser');
const parser = new Parser();


function attachClock(req, res, next){

    let {clock} = vclock.increment();

    for(let i=0; i<req.body.queries.length; i++){
        let query = req.body.queries[i];
        let ast = parser.astify(query);
        if(ast.type === "insert"){
            ast.columns.push("CENTRAL");
            ast.columns.push("L1980");
            ast.columns.push("GE1980");
            ast.columns.push("tombstone");
            for(let val of ast.values){
                val.value.push({type: 'number', value: clock.CENTRAL});
                val.value.push({type: 'number', value: clock.L1980});
                val.value.push({type: 'number', value: clock.GE1980});
                val.value.push({type: 'bool', value: false});
            }

            let newSQL = parser.sqlify(ast) + " AS new ON DUPLICATE KEY UPDATE";

            for(let i=0; i<ast.columns.length;i++){
                let col = ast.columns[i];
                if(i === ast.columns.length-1){
                    newSQL = `${newSQL} ${col} = new.${col}`;
                    continue;
                }
                newSQL = `${newSQL} ${col} = new.${col},`;
            }
            
            req.body.queries[i] = newSQL;
            continue;
        }
        else if(ast.type === "update"){
            ast.set.push({ column: "CENTRAL", value: {type: 'number', value: clock.CENTRAL}, table: null});
            ast.set.push({ column: "L1980", value: {type: 'number', value: clock.L1980}, table: null});
            ast.set.push({ column: "GE1980", value: {type: 'number', value: clock.GE1980}, table: null});
            req.body.queries[i] = parser.sqlify(ast);
            continue;
        }
        else if(ast.type === "delete"){
            let { where, table, orderby, limit, from } = ast;
            let newAst = {
                type: "update",
                where, from,
                table, orderby, limit,
                set: [ 
                    { column: "CENTRAL", value: {type: 'number', value: clock.CENTRAL}, table: null},
                    { column: "L1980", value: {type: 'number', value: clock.L1980}, table: null},
                    { column: "GE1980", value: {type: 'number', value: clock.GE1980}, table: null},
                    { column: "tombstone", value: {type: 'bool', value: true}, table: null}
                ]
            }
            req.body.queries[i] = parser.sqlify(newAst);
            continue;
        }
        else if(ast.type === "select"){
            if(ast.where){
                let userConditions = {...ast.where};
                ast.where = {
                    type: 'binary_expr',
                    operator: 'AND',
                    left: userConditions,
                    right: {
                        type: 'unary_expr',
                        operator: 'NOT',
                        expr: { type: 'column_ref', table: null, column: 'tombstone' }
                    }
                }
            }
            else{
                ast.where = {
                    type: 'unary_expr',
                    operator: 'NOT',
                    expr: { type: 'column_ref', table: null, column: 'tombstone' }
                }
            }

            req.body.queries[i] = parser.sqlify(ast);
        }

    }

    next();
}

module.exports = attachClock;