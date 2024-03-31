package ed.inf.adbs.lightdb;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;

class ProjectOperator extends ScanOperator {
    List<SelectItem<?>> selectItems;
    List<String> schema;

    public ProjectOperator(String inputFile, List<String> schema, List<SelectItem<?>> selectItems) throws Exception {
        super(inputFile);
        this.selectItems = selectItems;
        this.schema = schema;
    }

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
                newTuple += ",";
            }
            String columnNameWithTable = selectItem.toString();
            String columnName = columnNameWithTable.split("\\.")[1];
            int index = schema.indexOf(columnName);
            newTuple += tupleItem.get(index);
        }

        return newTuple;
    }
}