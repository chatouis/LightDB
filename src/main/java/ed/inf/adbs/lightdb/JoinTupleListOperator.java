package ed.inf.adbs.lightdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;

public class JoinTupleListOperator extends Operator{
    String[] tableNameList;
    String[] inputFileList;
    ScanOperator[] scanOperatorList;
    List<List<String>> schema;
    Expression whereExpression;
    Expression[] runningExpressions;
    String[] tuples;
    Boolean[] fullScaned;
    int tableNum;
    List<List<String>> tupleItemList;

    public JoinTupleListOperator(String databaseDir, Expression whereExpression, Table table, List<Join> joins, Map<String, List<String>> schema) throws Exception {
        tableNum = joins.size() + 1;
        tableNameList = new String[tableNum];
        inputFileList = new String[tableNum];
        scanOperatorList = new ScanOperator[tableNum];
        this.schema = new ArrayList<List<String>>();
        this.whereExpression = whereExpression;
        fullScaned = new Boolean[tableNum];

        tableNameList[0] = table.toString();
        for (int i = 0; i < joins.size(); i++) {
            tableNameList[i + 1] = joins.get(i).toString();
        }

        for (int i = 0; i < tableNum; i++) {
            inputFileList[i] = databaseDir + "/data/" + tableNameList[i] + ".csv";
            scanOperatorList[i] = new ScanOperator(inputFileList[i]);
            this.schema.add(schema.get(tableNameList[i]));
            fullScaned[i] = false;
        }

        tupleItemList = new ArrayList<List<String>>();
        String[] tuples = new String[tableNum];
        while (fullScaned() == false) {
            getNextTupleRecursive(tuples, 0);
        }
    }

    public Boolean fullScaned() {
        for (Boolean scaned : fullScaned) {
            if (!scaned) {
                return false;
            }
        }
        return true;
    }
    
    // dfs on tables
    private void getNextTupleRecursive(String[] tuples, int level) throws IOException {
        if (level == tableNum) {
            List<String> tupleItem = new ArrayList<String>();
            for (String tuple : tuples) {
                tupleItem.add(tuple);
            }
            // System.out.println(tuple);
            tupleItemList.add(tupleItem);
            return ;
        }

        ScanOperator currentScanOperator = scanOperatorList[level];
        String tuple;
        while ((tuple = currentScanOperator.getNextTuple()) != null) {
            tuples[level] = tuple;
            getNextTupleRecursive(tuples, level + 1);
        }
        fullScaned[level] = true;
        currentScanOperator.reset();
    }

    public String tupleItemToTuple(List<String> tupleItem) {
        String tuple = "";
        for (String item : tupleItem) {
            if (tuple.length() > 0) {
                tuple += ", ";
            }
            tuple += item;
        }
        return tuple;
    }

    @Override
    public String getNextTuple() throws IOException {
        if (tupleItemList.size() > 0) {
            List<String> tuple = tupleItemList.remove(0);
            EvaluateConditionTupleList deparser = new EvaluateConditionTupleList(schema, whereExpression, tuple) {};
            deparser.setBuffer(new StringBuilder());
            whereExpression.accept(deparser);
            while (deparser.value == false && tupleItemList.size() > 0) {
                tuple = tupleItemList.remove(0);
                deparser = new EvaluateConditionTupleList(schema, whereExpression, tuple) {};
                deparser.setBuffer(new StringBuilder());
                whereExpression.accept(deparser);    
            }

            if (tupleItemList.size() > 0) {
                return tupleItemToTuple(tuple);
            }
        }
        return null;
    }

    @Override
    public void reset() throws IOException {
        for (ScanOperator scanOperator : scanOperatorList) {
            scanOperator.reset();
        }
    }


}
