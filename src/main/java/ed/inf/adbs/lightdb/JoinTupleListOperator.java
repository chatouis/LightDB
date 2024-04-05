package ed.inf.adbs.lightdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;

public class JoinTupleListOperator extends Operator{
    String[] tableNameList;
    String[] inputFileList;
    ScanOperator[] scanOperatorList;
    protected List<List<String>> schema;
    Expression whereExpression;
    Expression[] runningExpressions;
    protected String[] tuples;
    Boolean[] fullScaned;
    protected int tableNum;
    protected List<List<String>> tupleItemList;
    Map<String, List<BinaryExpression>> tableToWhereExpression;

    public JoinTupleListOperator(String databaseDir, Expression whereExpression, Table table, List<Join> joins, 
            Map<String, List<String>> schema) throws Exception {
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

    public JoinTupleListOperator(String databaseDir, Expression whereExpression, List<String> tableNameList, Map<String, List<String>> schema, Map<String, List<BinaryExpression>> tableToWhereExpression) throws IOException {
        // for avoiding cross products
        tableNum = tableNameList.size();
        this.tableNameList = new String[tableNum];
        inputFileList = new String[tableNum];
        scanOperatorList = new ScanOperator[tableNum];
        this.schema = new ArrayList<List<String>>();
        this.whereExpression = whereExpression;
        fullScaned = new Boolean[tableNum];

        for (int i = 0; i < tableNum; i++) {
            this.tableNameList[i] = tableNameList.get(i);
            inputFileList[i] = databaseDir + "/data/" + this.tableNameList[i] + ".csv";
            scanOperatorList[i] = new ScanOperator(inputFileList[i]);
            this.schema.add(schema.get(this.tableNameList[i]));
            fullScaned[i] = false;
        }

        tupleItemList = new ArrayList<List<String>>();
        String[] tuples = new String[tableNum];
        this.tableToWhereExpression = tableToWhereExpression;
        while (fullScaned() == false) {
            getNextTupleRecursive(tuples, 0);
        }
    }

    public JoinTupleListOperator(String databaseDir, Expression whereExpression, Table table, List<Join> joins,
            Map<String, List<String>> schema, List<String> aliases) throws Exception {
        // for aliases
        tableNum = joins.size() + 1;
        tableNameList = new String[tableNum];
        inputFileList = new String[tableNum];
        scanOperatorList = new ScanOperator[tableNum];
        this.schema = new ArrayList<List<String>>();
        this.whereExpression = whereExpression;
        fullScaned = new Boolean[tableNum];

        tableNameList[0] = table.toString().split(" ")[0];
        for (int i = 0; i < joins.size(); i++) {
            tableNameList[i + 1] = joins.get(i).toString().split(" ")[0];
        }

        for (int i = 0; i < tableNum; i++) {
            inputFileList[i] = databaseDir + "/data/" + tableNameList[i] + ".csv";
            scanOperatorList[i] = new ScanOperator(inputFileList[i]);
            // this.schema.add(schema.get(tableNameList[i]));
            String alias = aliases.get(i);
            if (alias != "") {
                this.schema.add(
                    schema.get(tableNameList[i])
                        .stream()
                        .map(columnName -> alias + "." + columnName)
                        .collect(Collectors.toList())   
                        );
            }
            else {
                this.schema.add(schema.get(tableNameList[i]));
            }
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
    
    public BinaryExpression combineExpression(List<BinaryExpression> expressions) {
        BinaryExpression expression = expressions.get(0);
        for (int i = 0; i < expressions.size()-1; i++) {
            expressions.get(i).setRightExpression(expressions.get(i+1));
        }
        return expression;
    }

    // dfs on tables
    public void getNextTupleRecursive(String[] tuples, int level) throws IOException {
        if (tableToWhereExpression != null && level > 0) {
            // exist error
            // todo : combine expression
            List<String> tuple = Arrays.asList(tuples);
            List<String> filteredTuple = tuple.stream()
                                            .filter(s -> s != null)
                                            .collect(Collectors.toList());
            EvaluateConditionTupleList deparser = new EvaluateConditionTupleList(schema, whereExpression, filteredTuple) {};
            deparser.setBuffer(new StringBuilder());
            whereExpression.accept(deparser);
            if (deparser.value == false) {
                return;
            }
        }

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
