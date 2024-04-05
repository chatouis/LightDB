package ed.inf.adbs.lightdb.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ed.inf.adbs.lightdb.SelectOperator;
import ed.inf.adbs.lightdb.TupleComparator;
import net.sf.jsqlparser.expression.Expression;

public class SortSelectOperator extends SelectOperator {
    List<String> allTuple;
    List<String> schema;
    String orderBy;
    
    public SortSelectOperator(String inputFile, Expression whereExpression, List<String> schema, String orderBy) throws IOException {
        super(inputFile, whereExpression, schema);
        this.schema = schema;
        this.orderBy = orderBy;
        allTuple = new ArrayList<String>();
    }

    @Override
    public String getNextTuple() throws IOException {
        return super.getNextTuple();
    }

    @Override
    public void dump() throws IOException{
        String tuple;
        while ((tuple = getNextTuple()) != null) {
            allTuple.add(tuple);
        }
        Collections.sort(allTuple, new TupleComparator(schema, orderBy));
        for (String t : allTuple) {
            System.out.println(t);
        }
    }
    
}
