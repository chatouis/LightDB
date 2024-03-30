package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.*;

import ed.inf.adbs.lightdb.Operator.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

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

		// parsingExample(inputFile);
		
		try {
			Map<String, List<String>> schema = loadSchema(databaseDir + "/schema.txt");

			Table table = parsingForScan(inputFile);
			assert schema.containsKey(table.getName()) : "Table not found in schema";
			ScanOperator scanOperator = new ScanOperator(databaseDir + "/data/" + table + ".csv");
			scanOperator.dump();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Map<String, List<String>> loadSchema(String schemaFile) throws IOException{
		Map<String, List<String>> schema = new TreeMap<String, List<String>>();
		BufferedReader reader = new BufferedReader(new FileReader(schemaFile));
		String line;
		while ((line = reader.readLine()) != null) {
			List<String> parts = Arrays.asList(line.split(" "));
			schema.put(parts.get(0), parts.subList(0, parts.size()));
		}
		reader.close();
		return schema;
	}

	public static Table parsingForScan(String queryFile) throws FileNotFoundException, JSQLParserException {
		Statement statement = CCJSqlParserUtil.parse(new FileReader(queryFile));
		if (statement != null) {
			PlainSelect plainSelect = (PlainSelect) statement;
			return (Table) plainSelect.getFromItem();
		}
		return null;
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
				Table table = (Table) plainSelect.getFromItem();
				logger.log(Level.INFO, "Table name: " + table.getName());
				EqualsTo equalsTo = (EqualsTo) plainSelect.getWhere();
				Column a = (Column) equalsTo.getLeftExpression();
				logger.log(Level.INFO, "Right expression: " + a);
				Column b = (Column) equalsTo.getRightExpression();
				logger.log(Level.INFO, "Right expression: " + b);

				logger.log(Level.INFO, "----------------------------------");

				// 3. use net.sf.jsqlparser.util to extract table names, where clause, etc.
				TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
				logger.log(Level.INFO, "Table names: " + tablesNamesFinder.getTableList(statement));			

				ExpressionDeParser expr = new ExpressionDeParser();
				Expression where = plainSelect.getWhere();
				where.accept(expr);
				logger.log(Level.INFO, "Where clause: " + expr.getBuffer().toString());

				plainSelect.getWhere().accept(new ExpressionVisitorAdapter() {
					@Override
					public void visit(Column column) {
						column.setColumnName(column.getColumnName().replace("_", ""));
					}
				});
				logger.log(Level.INFO, "Where clause: " + plainSelect.getWhere().toString());

			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

}
