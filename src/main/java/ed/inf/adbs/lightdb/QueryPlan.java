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
			List<Join> joins = Utils.parsingJoins(inputSQL);
			Expression where = Utils.parsingWhere(inputSQL);
			List<SelectItem<?>> selectItems = Utils.parsingSelectItems(inputSQL);
			List<String> aliases = Utils.parsingAliases(inputSQL);

			String tableName = fromItem.toString();
			if (aliases != null) {
				tableName = tableName.split(" ")[0];
			}
			
			Utils.logger.log(Level.INFO, "----------------------------------");
			
			if (fromItem != null && where == null && joins == null && hasProjection(selectItems) == false) {
				ScanOperator scanOperator = new ScanOperator(databaseDir + "/data/" + tableName + ".csv");
				scanOperator.dump();
			}

			Utils.logger.log(Level.INFO, "----------------------------------");

			if (fromItem != null && where != null && joins == null && hasProjection(selectItems) == false){
				SelectOperator selectOperator = new SelectOperator(
					databaseDir + "/data/" + tableName + ".csv", where, schema.get(tableName));
				selectOperator.dump();
			}

			Utils.logger.log(Level.INFO, "----------------------------------");

			if (fromItem != null && hasProjection(selectItems) == true  && where == null && joins == null ) {
				ProjectOperator projectOperator = new ProjectOperator(
					databaseDir + "/data/" + tableName + ".csv", schema.get(tableName), selectItems);
				projectOperator.dump();
			}

			if (fromItem != null && hasProjection(selectItems) == true && where != null && joins == null ) {
				ProjectSelectOperator projectSelectOperator = new ProjectSelectOperator(
					databaseDir + "/data/" + tableName + ".csv", where, schema.get(tableName), selectItems);
				projectSelectOperator.dump();
			}

			Utils.logger.log(Level.INFO, "----------------------------------");

			if (fromItem != null && hasProjection(selectItems) == false && joins != null && where != null && joins.size() == 1) {
				String joinName = joins.get(0).toString().split(" ")[0];
				JoinOperator joinOperator;
				if (aliases == null) {
					joinOperator = new JoinOperator(databaseDir + "/data/" + tableName + ".csv", databaseDir + "/data/" + joinName + ".csv", where, schema);
				}
				else {
					String tableAlias = fromItem.toString().split(" ")[1];
					String joinAlias = joins.get(0).toString().split(" ")[1];
					joinOperator = new JoinOperator(databaseDir + "/data/" + tableName + ".csv", databaseDir + "/data/" + joinName + ".csv", where, schema, tableAlias, joinAlias);
				}
				joinOperator.dump();
			}

			if (fromItem != null && hasProjection(selectItems) == false && joins != null && where != null && joins.size() > 1) {
				// for avoiding cross products
				// -----------------
				// collectBinaryExpressions((BinaryExpression) where);
				// tableHashWhereExpression();
				// sortTable((Table)fromItem, joins);
				// JoinTupleListOperator joinTupleListOperator = new JoinTupleListOperator(databaseDir, where, tableNameList, schema, tableToWhereExpression);
				// -----------------

				JoinTupleListOperator joinTupleListOperator;
				if (aliases == null) {
					joinTupleListOperator = new JoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema);
				}
				else {
					joinTupleListOperator = new JoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema, aliases);
				}
				joinTupleListOperator.dump();
			}

			if (fromItem != null && hasProjection(selectItems) == true && joins != null && where != null && joins.size() > 1) {
				ProjectJoinTupleListOperator projectJoinTupleListOperator;
				if (aliases == null) {
					projectJoinTupleListOperator = new ProjectJoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema, selectItems);
				}
				else {
					projectJoinTupleListOperator = new ProjectJoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema, selectItems, aliases);
				}
				projectJoinTupleListOperator.dump();
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
