package ed.inf.adbs.lightdb;

import java.io.IOException;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.Join;

public class JoinOperator extends SelectOperator{

    public JoinOperator(String string, Expression where, List<String> schema, List<Join> joins) throws IOException {
        super(string, where, schema);
    }
    
}
