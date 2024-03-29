package ed.inf.adbs.lightdb;

import java.io.FileReader;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;

import java.util.logging.*;

/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {
	private static final Logger logger = Logger.getLogger(LightDB.class.getName());

	public static void main(String[] args) {
		logger.setLevel(Level.INFO);
		
		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		// Just for demonstration, replace this function call with your logic
		parsingExample(inputFile);
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */

	public static void parsingExample(String filename) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
//            Statement statement = CCJSqlParserUtil.parse("SELECT * FROM Boats");
			if (statement != null) {
				PlainSelect plainSelect = (PlainSelect) statement;
				logger.log(Level.INFO, "Read statement: " + statement);

				// 1. directly parse the SQL query
				logger.log(Level.INFO, "the first table: " + plainSelect.getFromItem());	
				logger.log(Level.INFO, "other tables: " + plainSelect.getJoins());
				logger.log(Level.INFO, "Where:" + plainSelect.getWhere());
				logger.log(Level.INFO, "Distinct:  " + plainSelect.getDistinct());
				logger.log(Level.INFO, "OrderByElements: " + plainSelect.getOrderByElements());
				logger.log(Level.INFO, "GroupByElements: " + plainSelect.getGroupBy());

				logger.log(Level.INFO, "----------------------------------");

				// 2. use expression to extract info
				// Table table = (Table) plainSelect.getFromItem();
				// logger.log(Level.INFO, "Table name: " + table.getName());
				// EqualsTo equalsTo = (EqualsTo) plainSelect.getWhere();
				// Column a = (Column) equalsTo.getLeftExpression();
				// logger.log(Level.INFO, "Right expression: " + a);
				// Column b = (Column) equalsTo.getRightExpression();
				// logger.log(Level.INFO, "Right expression: " + b);

				logger.log(Level.INFO, "----------------------------------");

				// 3. use net.sf.jsqlparser.util to extract table names, where clause, etc.
				// TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
				// logger.log(Level.INFO, "Table names: " + tablesNamesFinder.getTableList(statement));			

				// ExpressionDeParser expr = new ExpressionDeParser();
				// Expression where = plainSelect.getWhere();
				// where.accept(expr);
				// logger.log(Level.INFO, "Where clause: " + expr.getBuffer().toString());

				// plainSelect.getWhere().accept(new ExpressionVisitorAdapter() {
				// 	@Override
				// 	public void visit(Column column) {
				// 		column.setColumnName(column.getColumnName().replace("_", ""));
				// 	}
				// });
				// logger.log(Level.INFO, "Where clause: " + plainSelect.getWhere().toString());

			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}


public abstract class Operator {
    public abstract List<String> getNextTuple();
    public abstract void reset();
    public abstract SchemaDto getSchema();
}

