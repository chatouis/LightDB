package ed.inf.adbs.lightdb.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ed.inf.adbs.lightdb.ProjectSelectOperator;
import ed.inf.adbs.lightdb.TupleComparator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;


public class SortProjectSelectOperator extends ProjectSelectOperator  {
    List<String> allTuple;
    List<String> schema;
    String orderBy;

    public SortProjectSelectOperator(String inputFile, Expression whereExpression, List<String> schema, List<SelectItem<?>> selectItems, String orderBy) throws Exception {
        super(inputFile, whereExpression, schema, selectItems);
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
