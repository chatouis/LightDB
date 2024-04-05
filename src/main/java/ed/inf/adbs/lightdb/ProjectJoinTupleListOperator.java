package ed.inf.adbs.lightdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectJoinTupleListOperator extends JoinTupleListOperator{
    List<SelectItem<?>> selectItems;
    public List<String> schema;
    
    public ProjectJoinTupleListOperator(String databaseDir, Expression whereExpression, Table table, List<Join> joins,
             Map<String, List<String>> schema, List<SelectItem<?>> selectItems) throws Exception {
        super(databaseDir, whereExpression, table, joins, schema);
        this.selectItems = selectItems;
        this.schema = new ArrayList<String>();
        this.schema.addAll(schema.get(table.toString()));
        for (Join join : joins) {
            this.schema.addAll(schema.get(join.toString()));
        }
    }

    public ProjectJoinTupleListOperator(String databaseDir, Expression whereExpression, Table table, List<Join> joins,
            Map<String, List<String>> schema, List<SelectItem<?>> selectItems, List<String> aliases) throws Exception {
        super(databaseDir, whereExpression, table, joins, schema, aliases);
        this.selectItems = selectItems;
        this.schema = new ArrayList<String>();
        this.schema.addAll(schema.get(table.toString().split(" ")[0]));
        for (Join join : joins) {
            this.schema.addAll(schema.get(join.toString().split(" ")[0]));
        }
    }

    @Override
    public String getNextTuple() throws IOException {
        String tuple = super.getNextTuple();
        if (tuple == null) {
            return null;
        }
        
        String[] _tupleItem = tuple.split(",");
        // strip 
        for (int i = 0; i < _tupleItem.length; i++) {
            _tupleItem[i] = _tupleItem[i].trim();
        }
        List<String> tupleItem = Arrays.asList(_tupleItem);

        String newTuple = "";
        for (SelectItem<?> selectItem : selectItems) {
            if (newTuple.length() > 0) {
                newTuple += ", ";
            }
            String columnNameWithTable = selectItem.toString();
            String columnName = columnNameWithTable.split("\\.")[1];
            int index = schema.indexOf(columnName);
            newTuple += tupleItem.get(index);
        }

        return newTuple;
    }
    
}
