package ed.inf.adbs.lightdb.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import ed.inf.adbs.lightdb.JoinOperator;
import ed.inf.adbs.lightdb.TupleComparator;
import net.sf.jsqlparser.expression.Expression;


public class SortJoinOperator extends JoinOperator{
    List<String> allTuple;
    List<String> schema;
    String orderBy;

    public SortJoinOperator(String leftInputFile, String rightIputFile, Expression whereExpression, Map<String, List<String>> schema, String orderBy) throws IOException {
        super(leftInputFile, rightIputFile, whereExpression, schema);
        this.schema = new ArrayList<String>();
        String leftTableName = pathToTableName(leftInputFile);
        String rightTableName = pathToTableName(rightIputFile);
        this.schema.addAll(schema.get(leftTableName));
        this.schema.addAll(schema.get(rightTableName));
        this.orderBy = orderBy;
        allTuple = new ArrayList<String>();
    }

    public SortJoinOperator(String leftInputFile, String rightIputFile, Expression whereExpression, Map<String, List<String>> schema, String tableAlias, String joinAlias, String orderBy) throws Exception {
        super(leftInputFile, rightIputFile, whereExpression, schema, tableAlias, joinAlias);
        this.schema = new ArrayList<String>();
        String leftTableName = pathToTableName(leftInputFile);
        String rightTableName = pathToTableName(rightIputFile);
        if (tableAlias != "") {
            this.schema.addAll(schema.get(leftTableName).stream().map(columnName -> tableAlias + "." + columnName).collect(Collectors.toList()));
        }
        else {
            this.schema.addAll(schema.get(leftTableName));
        }
        if (joinAlias != "") {
            this.schema.addAll(schema.get(rightTableName).stream().map(columnName -> joinAlias + "." + columnName).collect(Collectors.toList()));
        }
        else {
            this.schema.addAll(schema.get(rightTableName));
        }

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
        orderBy = schema.contains(orderBy) ? orderBy : orderBy.split("\\.")[1];
        Collections.sort(allTuple, new TupleComparator(schema, orderBy));
        for (String t : allTuple) {
            System.out.println(t);
        }
    }
}
