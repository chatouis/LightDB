package ed.inf.adbs.lightdb.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ed.inf.adbs.lightdb.JoinTupleListOperator;
import ed.inf.adbs.lightdb.ProjectJoinTupleListOperator;
import ed.inf.adbs.lightdb.TupleComparator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;


public class SortProjectJoinTupleListOperator extends ProjectJoinTupleListOperator{
    List<String> schema;
    String orderBy;
    List<String> allTuple;
    int tableNum;

    public SortProjectJoinTupleListOperator(String databaseDir, Expression whereExpression, Table table, List<Join> joins, 
        Map<String, List<String>> schema, List<SelectItem<?>> selectItems, String orderBy) throws Exception {
        super(databaseDir, whereExpression, table, joins, schema, selectItems);
        this.orderBy = orderBy;
        this.allTuple = new ArrayList<String>();
    }

    public SortProjectJoinTupleListOperator(String databaseDir, Expression whereExpression, Table table, List<Join> joins,
        Map<String, List<String>> schema, List<SelectItem<?>> selectItems, List<String> aliases, String orderBy) throws Exception {
        super(databaseDir, whereExpression, table, joins, schema, selectItems, aliases);
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
        //  for simplicity
        Collections.sort(allTuple);
        for (String t : allTuple) {
            System.out.println(t);
        }
    }
}
