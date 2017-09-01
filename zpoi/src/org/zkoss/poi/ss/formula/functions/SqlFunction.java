package org.zkoss.poi.ss.formula.functions;


import org.model.DBHandler;
import org.zkoss.poi.ss.formula.eval.*;

import java.sql.*;
import java.util.ArrayList;

public class SqlFunction implements Function {

    @Override
    public ValueEval evaluate(ValueEval[] args, int srcRowIndex, int srcColumnIndex) {
        int nArgs = args.length;
        if (nArgs < 1) {
            // too few arguments
            return ErrorEval.VALUE_INVALID;
        }

        if (nArgs > 30) {
            // too many arguments
            return ErrorEval.VALUE_INVALID;
        }
        
        ValueEval evaluatedQuery = evaluateQueryArg(args[0], srcRowIndex, srcColumnIndex);
        ValueEval[] evaluatedParameters = evaluateParameterArgs(args, srcRowIndex, srcColumnIndex);
        
        
        if (evaluatedQuery instanceof StringEval) {
            try {
                String queryString = ((StringEval) evaluatedQuery).getStringValue();
                return makeQuery(queryString, evaluatedParameters);
            } catch (SQLException e) {
                e.printStackTrace();
                return ErrorEval.VALUE_INVALID;
            }
        }
        throw new RuntimeException("Unexpected type for query ("
                + evaluatedQuery.getClass().getName() + ")");
    }

    private ArrayEval parseResult(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int numCols = metaData.getColumnCount();
        ArrayList<StringEval[]> rows = new ArrayList<>();
        
        while (rs.next()) {
            StringEval[] row = new StringEval[numCols];
            for (int i = 0; i < numCols; i++) {
                row[i] = new StringEval(rs.getString(i+1));
            }
            rows.add(row);
        }

        int numRows = rows.size();
        
        StringEval[][] evalSourceArray = new StringEval[rows.size()][];
        for (int i = 0; i < numRows; i++) {
            evalSourceArray[i] = rows.get(i);
        }

        ArrayEval eval = new ArrayEval(evalSourceArray, 0, 0, numRows-1, numCols-1, null);
        return eval;
    }

    private ValueEval makeQuery(String queryString, ValueEval[] parameters) throws SQLException {
        Connection connection = DBHandler.instance.getConnection();
        
        PreparedStatement stmt = connection.prepareStatement(queryString);
        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i] instanceof StringEval) {
                String paramString = ((StringEval) parameters[i]).getStringValue();
                stmt.setString(i+1, paramString);
            }
        }
        
        ResultSet result = stmt.executeQuery();
        ArrayEval resultEval = parseResult(result);
        connection.commit();
        connection.close();
        return resultEval;
    }

    /**
     *
     * @return the de-referenced query arg (possibly {@link ErrorEval})
     */
    private static ValueEval evaluateQueryArg(ValueEval arg, int srcRowIndex, int srcColumnIndex) {
        try {
            return OperandResolver.getSingleValue(arg, srcRowIndex, (short)srcColumnIndex);
        } catch (EvaluationException e) {
            return e.getErrorEval();
        }
    }

    /**
     *
     * @return the de-referenced query arg (possibly {@link ErrorEval})
     */
    private static ValueEval[] evaluateParameterArgs(ValueEval[] args, int srcRowIndex, int srcColumnIndex) {
        ValueEval[] evaluatedArgs = new ValueEval[args.length-1];
        for (int i = 1; i < args.length; i++) {
            try {
                evaluatedArgs[i-1] = OperandResolver.getSingleValue(args[i], srcRowIndex, (short)srcColumnIndex);
            } catch (EvaluationException e) {
                evaluatedArgs[i-1] = e.getErrorEval();
            }
        }
        return evaluatedArgs;
    }

    
}
