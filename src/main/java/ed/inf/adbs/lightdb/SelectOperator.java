package ed.inf.adbs.lightdb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends ScanOperator{
    public List<String> schema;
    public Expression whereExpression;
    public List<String> tables;

    public SelectOperator(String inputFile, Expression whereExpression, List<String> schema) throws IOException {
        super(inputFile);
        this.whereExpression = whereExpression;
        this.schema = schema;
    }

    @Override
    public String getNextTuple() throws IOException {
        String tuple = super.getNextTuple();
        if (tuple == null) {
            return null;
        }

        EvaluateCondition deparser = new EvaluateCondition(schema, whereExpression, tuple) {};
        deparser.setBuffer(new StringBuilder());
        whereExpression.accept(deparser);
        if (deparser.value) {
            return tuple;
        }
        else {
            return getNextTuple();
        }
    }

}


