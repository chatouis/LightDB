package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import ed.inf.adbs.lightdb.sort.*;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Distinct;
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
			String orderBy = Utils.parsingOrderBy(inputSQL);
			Boolean distinct = Utils.parsingDistinct(inputSQL);


			String tableName = fromItem.toString();
			if (aliases != null) {
				tableName = tableName.split(" ")[0];
			}

			if (fromItem != null && where == null && joins == null && hasProjection(selectItems) == false) {
				if (orderBy == null) {
					ScanOperator scanOperator = new ScanOperator(databaseDir + "/data/" + tableName + ".csv");
					scanOperator.setDistinct(distinct);
					scanOperator.dump();
				}
				else {
					orderBy = orderBy.split("\\.")[1];
					SortScanOperator sortScanOperator = new SortScanOperator(
						databaseDir + "/data/" + tableName + ".csv", orderBy, schema.get(tableName));
					sortScanOperator.setDistinct(distinct);
					sortScanOperator.dump();
				}
			}

			if (fromItem != null && where != null && joins == null && hasProjection(selectItems) == false){
				if (orderBy == null) {
					SelectOperator selectOperator = new SelectOperator(
						databaseDir + "/data/" + tableName + ".csv", where, schema.get(tableName));
					selectOperator.setDistinct(distinct);
					selectOperator.dump();
				}
				else {
					orderBy = orderBy.split("\\.")[1];
					SortSelectOperator sortSelectOperator = new SortSelectOperator(
						databaseDir + "/data/" + tableName + ".csv", where, schema.get(tableName), orderBy);
					sortSelectOperator.setDistinct(distinct);
					sortSelectOperator.dump();
				}
			}

			if (fromItem != null && hasProjection(selectItems) == true  && where == null && joins == null ) {
				if (orderBy == null) {
					ProjectOperator projectOperator = new ProjectOperator(
						databaseDir + "/data/" + tableName + ".csv", schema.get(tableName), selectItems);
					projectOperator.setDistinct(distinct);
					projectOperator.dump();
				}
				else {
					orderBy = orderBy.split("\\.")[1];
					SortProjectOperator sortProjectOperator = new SortProjectOperator(
						databaseDir + "/data/" + tableName + ".csv", schema.get(tableName), selectItems, orderBy);
					sortProjectOperator.setDistinct(distinct);
					sortProjectOperator.dump();
				}
			}

			if (fromItem != null && hasProjection(selectItems) == true && where != null && joins == null ) {
				if (orderBy == null) {
					ProjectSelectOperator projectSelectOperator = new ProjectSelectOperator(
						databaseDir + "/data/" + tableName + ".csv", where, schema.get(tableName), selectItems);
					projectSelectOperator.setDistinct(distinct);
					projectSelectOperator.dump();
				}
				else {
					orderBy = orderBy.split("\\.")[1];
					SortProjectSelectOperator sortProjectSelectOperator = new SortProjectSelectOperator(
						databaseDir + "/data/" + tableName + ".csv", where, schema.get(tableName), selectItems, orderBy);
					sortProjectSelectOperator.setDistinct(distinct);
					sortProjectSelectOperator.dump();
				}
			}

			if (fromItem != null && hasProjection(selectItems) == false && joins != null && where != null && joins.size() == 1) {
				if (orderBy == null) {
					String joinName = joins.get(0).toString().split(" ")[0];
					JoinOperator joinOperator;
					if (aliases == null) {
						joinOperator = new JoinOperator(databaseDir + "/data/" + tableName + ".csv", databaseDir + "/data/" + joinName + ".csv", where, schema);
					}
					else {
						String tableAlias = fromItem.toString().split(" ").length == 2 ? fromItem.toString().split(" ")[1] : "";
						String joinAlias = joins.get(0).toString().split(" ").length == 2 ? joins.get(0).toString().split(" ")[1] : "";
						joinOperator = new JoinOperator(databaseDir + "/data/" + tableName + ".csv", databaseDir + "/data/" + joinName + ".csv", where, schema, tableAlias, joinAlias);
					}
					joinOperator.setDistinct(distinct);
					joinOperator.dump();
				}
				else {
					String joinName = joins.get(0).toString().split(" ")[0];
					SortJoinOperator sortJoinOperator;
					if (aliases == null) {
						sortJoinOperator = new SortJoinOperator(databaseDir + "/data/" + tableName + ".csv", databaseDir + "/data/" + joinName + ".csv", where, schema, orderBy);
					}
					else {
						String tableAlias = fromItem.toString().split(" ").length == 2 ? fromItem.toString().split(" ")[1] : "";
						String joinAlias = joins.get(0).toString().split(" ").length == 2 ? joins.get(0).toString().split(" ")[1] : "";
						sortJoinOperator = new SortJoinOperator(databaseDir + "/data/" + tableName + ".csv", databaseDir + "/data/" + joinName + ".csv", where, schema, tableAlias, joinAlias, orderBy);
					}
					sortJoinOperator.setDistinct(distinct);
					sortJoinOperator.dump();
				}

			}

			if (fromItem != null && hasProjection(selectItems) == false && joins != null && where != null && joins.size() > 1) {
				// for avoiding cross products
				// -----------------
				// collectBinaryExpressions((BinaryExpression) where);
				// tableHashWhereExpression();
				// sortTable((Table)fromItem, joins);
				// JoinTupleListOperator joinTupleListOperator = new JoinTupleListOperator(databaseDir, where, tableNameList, schema, tableToWhereExpression);
				// -----------------
				if (orderBy == null) {
					JoinTupleListOperator joinTupleListOperator;
					if (aliases == null) {
						joinTupleListOperator = new JoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema);
					}
					else {
						joinTupleListOperator = new JoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema, aliases);
					}
					joinTupleListOperator.setDistinct(distinct);
					joinTupleListOperator.dump();
				}
				else {
					SortJoinTupleListOperator sortJoinTupleListOperator;
					if (aliases == null) {
						sortJoinTupleListOperator = new SortJoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema, orderBy);
					}
					else {
						sortJoinTupleListOperator = new SortJoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema, aliases, orderBy);
					}
					sortJoinTupleListOperator.setDistinct(distinct);
					sortJoinTupleListOperator.dump();
				}

			}

			if (fromItem != null && hasProjection(selectItems) == true && joins != null && where != null && joins.size() > 1) {
				if (orderBy == null) {
					ProjectJoinTupleListOperator projectJoinTupleListOperator;
					if (aliases == null) {
						projectJoinTupleListOperator = new ProjectJoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema, selectItems);
					}
					else {
						projectJoinTupleListOperator = new ProjectJoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema, selectItems, aliases);
					}
					projectJoinTupleListOperator.setDistinct(distinct);
					projectJoinTupleListOperator.dump();	
				}
				else {
					SortProjectJoinTupleListOperator sortProjectJoinTupleListOperator;
					if (aliases == null) {
						sortProjectJoinTupleListOperator = new SortProjectJoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema, selectItems, orderBy);
					}
					else {
						sortProjectJoinTupleListOperator = new SortProjectJoinTupleListOperator(databaseDir, where, (Table)fromItem, joins, schema, selectItems, aliases, orderBy);
					}
					sortProjectJoinTupleListOperator.setDistinct(distinct);
					sortProjectJoinTupleListOperator.dump();
				}
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
