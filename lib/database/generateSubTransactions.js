const { Parser } = require('node-sql-parser');
const parser = new Parser();

function generateSubTransactions(query){
    try{
        let originalAst = parser.astify(query);
        let { values, columns } = originalAst;
        let index=columns.indexOf("year");
        if(index <= -1)
            index = 0;
        let valuesL1980 = [];
        let valuesGE1980 = [];

        for(let valueRow of values){
            let { value } = valueRow;
            if(value[index].value >= 1980){
                valuesGE1980.push(valueRow);
            }
            else{
                valuesL1980.push(valueRow);
            }
        }

        let astL1980 = {...originalAst, values: valuesL1980};
        let astGE1980 = {...originalAst, values: valuesGE1980};
        let subTransactions = {};
        if(valuesL1980.length > 0)
            subTransactions.L1980 = parser.sqlify(astL1980);
        if(valuesGE1980.length > 0)
            subTransactions.GE1980 = parser.sqlify(astGE1980)
        
        return subTransactions;
    }
    catch(e){
        throw e;
    }
}

module.exports = generateSubTransactions;