package ed.inf.adbs.lightdb.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.lightdb.JoinTupleListOperator;
import ed.inf.adbs.lightdb.TupleComparator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;


public class SortJoinTupleListOperator extends JoinTupleListOperator{
    List<String> schema;
    String orderBy;
    List<String> allTuple;
    int tableNum;

    public SortJoinTupleListOperator(String databaseDir, Expression whereExpression, Table table, List<Join> joins, 
            Map<String, List<String>> schema, String orderBy) throws Exception {
        super(databaseDir, whereExpression, table, joins, schema);
        this.orderBy = orderBy;
        this.allTuple = new ArrayList<String>();
    }

    public SortJoinTupleListOperator(String databaseDir, Expression whereExpression, Table table, List<Join> joins,
        Map<String, List<String>> schema, List<String> aliases, String orderBy) throws Exception {
        super(databaseDir, whereExpression, table, joins, schema, aliases);
        this.orderBy = orderBy;
        this.allTuple = new ArrayList<String>();
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
        Collections.sort(allTuple, new TupleComparator(super.schema, orderBy, super.tableNum));
        for (String t : allTuple) {
            System.out.println(t);
        }
    }
}
