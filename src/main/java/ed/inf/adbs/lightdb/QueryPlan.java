package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.SelectItem;

public class QueryPlan {
    String inputSQL;
    String databaseDir;
	List<BinaryExpression> binaryExpressions;
	List<String> tableNameList;
	Map<String, List<BinaryExpression>> tableToWhereExpression;
	Map<String, Integer> sortHeuristic;

    public QueryPlan(String inputSQL, String databaseDir) {
        this.inputSQL = inputSQL;
        this.databaseDir = databaseDir;
		binaryExpressions = new ArrayList<>();
		tableNameList = new ArrayList<>();
		tableToWhereExpression = new HashMap<>();
		sortHeuristic = new HashMap<>();
    }

	public boolean hasProjection(List<SelectItem<?>> selectItems) {
        for (SelectItem<?> selectItem : selectItems) {
            String columnNameWithTable = selectItem.toString();
            if (columnNameWithTable.contains(".")) {
				return true;
			}
        }
		return false;
	}

    public void execute() {
		try {
			Map<String, List<String>> schema = Utils.loadSchema(databaseDir + "/schema.txt");

			FromItem fromItem = Utils.parsingFromItem(inputSQL);
			// Set<String> tables = Utils.parsingTablesSet(inputSQL);
			Expression where = Utils.parsingWhere(inputSQL);
			List<SelectItem<?>> selectItems = Utils.parsingSelectItems(inputSQL);
			List<Join> joins = Utils.parsingJoins(inputSQL);

			Utils.logger.log(Level.INFO, "----------------------------------");
			
			if (fromItem != null && where == null && joins == null && hasProjection(selectItems) == false) {
				ScanOperator scanOperator = new ScanOperator(databaseDir + "/data/" + (Table)fromItem + ".csv");
				scanOperator.dump();
			}

			Utils.logger.log(Level.INFO, "----------------------------------");

			if (fromItem != null && where != null && joins == null && hasProjection(selectItems) == false){
				Table table = (Table)fromItem;
				SelectOperator selectOperator = new SelectOperator(
					databaseDir + "/data/" + table.toString() + ".csv", where, schema.get(table.toString()));
				selectOperator.dump();
			}

			Utils.logger.log(Level.INFO, "----------------------------------");

			if (fromItem != null && hasProjection(selectItems) == true  && where == null && joins == null ) {
				Table table = (Table)fromItem;
				ProjectOperator projectOperator = new ProjectOperator(
					databaseDir + "/data/" + table.toString() + ".csv", schema.get(table.toString()), selectItems);
				projectOperator.dump();
			}

			if (fromItem != null && hasProjection(selectItems) == true && where != null && joins == null ) {
				Table table = (Table)fromItem;
				ProjectSelectOperator projectSelectOperator = new ProjectSelectOperator(
					databaseDir + "/data/" + table.toString() + ".csv", where, schema.get(table.toString()), selectItems);
				projectSelectOperator.dump();
			}

			Utils.logger.log(Level.INFO, "----------------------------------");

			if (fromItem != null && hasProjection(selectItems) == false && joins != null && where != null && joins.size() == 1) {
				Table table = (Table)fromItem;
				JoinOperator joinOperator = new JoinOperator(
					databaseDir + "/data/" + table.toString() + ".csv", databaseDir + "/data/" + joins.get(0).toString() + ".csv", where, schema);
				joinOperator.dump();
			}

			if (fromItem != null && hasProjection(selectItems) == false && joins != null && where != null && joins.size() > 1) {
				// Expression where = sortExpression(inputSQL);
				collectBinaryExpressions((BinaryExpression) where);
				tableHashWhereExpression();
				sortTable((Table)fromItem, joins);
				// JoinTupleListOperator joinTupleListOperator = new JoinTupleListOperator(databaseDir, where, tableNameList, schema, tableToWhereExpression);
				JoinTupleListOperator joinTupleListOperator = new JoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema);
				joinTupleListOperator.dump();
			}


		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    public void collectBinaryExpressions(BinaryExpression expression) {
		if (expression.getLeftExpression() instanceof Column || expression.getRightExpression() instanceof Column) {
			binaryExpressions.add((BinaryExpression) expression);
		}
        else{
            collectBinaryExpressions((BinaryExpression) expression.getLeftExpression());
            collectBinaryExpressions((BinaryExpression) expression.getRightExpression());
        }
    }

	public void tableHashWhereExpression() {
		for (BinaryExpression binaryExpression : binaryExpressions) {
			if (binaryExpression.getLeftExpression() instanceof Column) {
				String tableName = binaryExpression.getLeftExpression().toString().split("\\.")[0];
				tableToWhereExpression.computeIfAbsent(tableName, k -> new ArrayList<>()).add(binaryExpression);
				if (hasLongValue(binaryExpression)) {
					sortHeuristic.put(tableName, sortHeuristic.getOrDefault(tableName, 0) + 1);
				}
			}
			if (binaryExpression.getRightExpression() instanceof Column) {
				String tableName = binaryExpression.getRightExpression().toString().split("\\.")[0];
				tableToWhereExpression.computeIfAbsent(tableName, k -> new ArrayList<>()).add(binaryExpression);
				if (hasLongValue(binaryExpression)) {
					sortHeuristic.put(tableName, sortHeuristic.getOrDefault(tableName, 0) + 1);
				}
			}
		}
	}

	public boolean hasLongValue(BinaryExpression expression) {
		if (expression.getLeftExpression() instanceof LongValue || expression.getRightExpression() instanceof LongValue) {
			return true;
		}
		return false;
	}

	public void sortTable(Table table, List<Join> joins) {
		tableNameList.add(table.toString());
		for (Join join : joins) {
			tableNameList.add(join.toString());
		}

		tableNameList.sort((a, b) -> { return sortHeuristic.getOrDefault(b, 0) - sortHeuristic.getOrDefault(a, 0);});

	}


}
