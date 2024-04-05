package ed.inf.adbs.lightdb;

import java.util.Comparator;
import java.util.List;



public class TupleComparator implements Comparator<String> {
    public int compareIndex = 0;
    int indexOfTable;
    int indexOfColoumn;

    public TupleComparator(List<String> schema, String orderBy) {
        orderBy = orderBy.split("\\.").length == 1 ? orderBy : orderBy.split("\\.")[1];
        compareIndex = schema.indexOf(orderBy);
    }

    public TupleComparator(List<List<String>> schemaList, String orderBy, int tableNum) {
        String columnName = orderBy.split("\\.")[1];
        String tableName = orderBy.split("\\.")[0];

        for (List<String> schema : schemaList) {
            // todo : support using alias and tableName at the same time
            if (schema.contains(columnName) == true) {
                //  search for correct table
                indexOfTable= schemaList.indexOf(schema);
                indexOfColoumn = schema.indexOf(columnName);
            }
            else if (schema.contains(tableName + "." + columnName) == true) {
                // consider alias
                indexOfTable= schemaList.indexOf(schema);
                indexOfColoumn = schema.indexOf(tableName + "." + columnName);
            }
        }

        for (int i = 0; i < indexOfTable; i++) {
            compareIndex += schemaList.get(i).size();
        }
        compareIndex += indexOfColoumn;

    }

    public String[] tupleToTupleItem(String tuple) {
        String[] tupleItem = tuple.split(",");
        // strip 
        for (int i = 0; i < tupleItem.length; i++) {
            tupleItem[i] = tupleItem[i].trim();
        }
        return tupleItem;
    }

    @Override
    public int compare(String tuple1, String tuple2) {
        String[] tupleItem1 = tupleToTupleItem(tuple1);
        String[] tupleItem2 = tupleToTupleItem(tuple2); 

        long value1 = Long.parseLong(tupleItem1[compareIndex]);
        long value2 = Long.parseLong(tupleItem2[compareIndex]);
    
        if (value1 < value2) {
            return -1;
        } else if (value1 == value2) {
            return 0;
        } else {
            return 1;
        }    
    }
}