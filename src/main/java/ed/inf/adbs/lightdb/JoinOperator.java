package ed.inf.adbs.lightdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;

public class JoinOperator extends Operator{
    Expression whereExpression;
    List<Join> joins;
    List<String> schema;
    ScanOperator leftScanOperator, rightScanOperator;
    String leftTuple = null;
    String rightTuple = null;

    public JoinOperator(String leftInputFile, String rightIputFile, Expression whereExpression, Map<String, List<String>> schema) throws IOException {
        leftScanOperator = new ScanOperator(leftInputFile);
        rightScanOperator = new ScanOperator(rightIputFile);
        this.whereExpression = whereExpression;
        String leftTableName = pathToTableName(leftInputFile);
        String rightTableName = pathToTableName(rightIputFile);
        this.schema = new ArrayList<String>();
        this.schema.addAll(schema.get(leftTableName));
        this.schema.addAll(schema.get(rightTableName));
    }

    public String pathToTableName(String path) {
        return path.split("/")[path.split("/").length - 1].split(".csv")[0];
    }

    public String getNextTuple() throws IOException {
        // rightTuple is faster pointer
        if (rightTuple == null) {
            leftTuple = leftScanOperator.getNextTuple();
            if (leftTuple == null) {
                return null;
            }
        }
        rightTuple = rightScanOperator.getNextTuple();
        if (rightTuple == null) {
            return null;
        }
        EvaluateCondition deparser = new EvaluateCondition(schema, whereExpression, leftTuple + "," + rightTuple) {};
        deparser.setBuffer(new StringBuilder());
        whereExpression.accept(deparser);
        while (deparser.value == false) {
            rightTuple = rightScanOperator.getNextTuple();
            // finish inner loop 
            if (rightTuple == null) {
                leftTuple = leftScanOperator.getNextTuple();
                rightScanOperator.reset();
                rightTuple = rightScanOperator.getNextTuple();
                // finish outer loop 
                if (leftTuple == null) {
                    return null;
                }
            }
            deparser = new EvaluateCondition(schema, whereExpression, leftTuple + "," + rightTuple) {};
            deparser.setBuffer(new StringBuilder());
            whereExpression.accept(deparser);
        }

    
        return leftTuple + ", " + rightTuple;
    }

    @Override
    public void reset() throws IOException {
        leftScanOperator.reset();
        rightScanOperator.reset();
    }
    
}
