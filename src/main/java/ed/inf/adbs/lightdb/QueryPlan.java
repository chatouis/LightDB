package ed.inf.adbs.lightdb;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

public class QueryPlan {
    String inputSQL;
    String databaseDir;

    public QueryPlan(String inputSQL, String databaseDir) {
        this.inputSQL = inputSQL;
        this.databaseDir = databaseDir;
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

			if (fromItem != null && hasProjection(selectItems) == true && joins == null && where == null ) {
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

			if (fromItem != null && joins != null && where != null) {
				Table table = (Table)fromItem;
				JoinOperator joinOperator = new JoinOperator(
					databaseDir + "/data/" + table.toString() + ".csv", databaseDir + "/data/" + joins.get(0).toString() + ".csv", where, schema);
				joinOperator.dump();
			}


		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
