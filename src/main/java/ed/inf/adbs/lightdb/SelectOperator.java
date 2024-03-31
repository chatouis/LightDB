package ed.inf.adbs.lightdb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends ScanOperator{
    public Map<String, List<String>> schema;
    public Expression whereExpression;
    public List<String> tables;

    public SelectOperator(String inputFile, List<String> tables, Expression whereExpression) throws IOException {
        super(inputFile);
        this.tables = tables;
        this.whereExpression = whereExpression;
    }

    @Override
    public String getNextTuple() throws IOException {
        String tuple = super.getNextTuple();
        if (tuple == null) {
            return null;
        }
        EvaluateCondition deparser = new EvaluateCondition(tables, whereExpression, tuple) {};
        deparser.setBuffer(new StringBuilder());
        whereExpression.accept(deparser);
        if (deparser.value) {
            return tuple;
        }
        return null;
    }

}


