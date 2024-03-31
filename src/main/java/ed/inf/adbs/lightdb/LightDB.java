package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

		String databaseDir = "samples/db";
		String inputSQL = "samples/input/query5.sql";
		String outputFile = "samples/output/query1.csv";

		// if (args.length != 3) {
		// 	System.err.println("Usage: LightDB database_dir input_file output_file");
		// 	return;
		// }

		// String databaseDir = args[0];
		// String inputSQL = args[1];
		// String outputFile = args[2];

		parsingExample(inputSQL);
		
		// try {
		// 	Map<String, List<String>> schema = loadSchema(databaseDir + "/schema.txt");

		// 	List<Join> tables = parsingTables(inputSQL);
		// 	Expression whereExpression = parsingWhere(inputSQL);

		// 	logger.log(Level.INFO, "----------------------------------");
			
		// 	// ScanOperator scanOperator = new ScanOperator(databaseDir + "/data/" + table + ".csv");
		// 	// scanOperator.dump();

		// 	logger.log(Level.INFO, "----------------------------------");

		// 	// SelectOperator selectOperator = new SelectOperator(tables, whereExpression);
		// 	// selectOperator.dump();

		// } catch (Exception e) {
		// 	e.printStackTrace();
		// }
	}

	public static Map<String, List<String>> loadSchema(String schemaFile) throws IOException{
		Map<String, List<String>> schema = new TreeMap<String, List<String>>();
		BufferedReader reader = new BufferedReader(new FileReader(schemaFile));
		String tuple;
		while ((tuple = reader.readLine()) != null) {
			List<String> parts = Arrays.asList(tuple.split(" "));
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

	public static List<Join> parsingTables(String queryFile) throws FileNotFoundException, JSQLParserException {
		Statement statement = CCJSqlParserUtil.parse(new FileReader(queryFile));
		if (statement != null) {
			// TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
			// return tablesNamesFinder.getTables(statement);
			PlainSelect plainSelect = (PlainSelect) statement;
			FromItem from = plainSelect.getFromItem();
			List<Join> joins = plainSelect.getJoins();
			return joins;

		}
		return null;
	}

	public static Expression parsingWhere(String queryFile) throws FileNotFoundException, JSQLParserException {
		Statement statement = CCJSqlParserUtil.parse(new FileReader(queryFile));
		if (statement != null) {
			PlainSelect plainSelect = (PlainSelect) statement;
			return plainSelect.getWhere();
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
				if (plainSelect.getWhere() instanceof EqualsTo) {
					EqualsTo equalsTo = (EqualsTo) plainSelect.getWhere();
					Column a = (Column) equalsTo.getLeftExpression();
					logger.log(Level.INFO, "Left expression: " + a);
					Column b = (Column) equalsTo.getRightExpression();
					logger.log(Level.INFO, "Right expression: " + b);
				}
				logger.log(Level.INFO, "----------------------------------");

				// 3. use net.sf.jsqlparser.util to extract table names, where clause, etc.
				TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
				logger.log(Level.INFO, "Table names: " + tablesNamesFinder.getTables(statement));		

				ExpressionDeParser deParser = new ExpressionDeParser();
				Expression where = plainSelect.getWhere();
				where.accept(deParser);
				logger.log(Level.INFO, "Where clause: " + deParser.getBuffer().toString());

				plainSelect.getWhere().accept(new ExpressionVisitorAdapter() {
					@Override
					public void visit(Column column) {
						column.setColumnName(column.getColumnName().replace("_", ""));
					}
				});
				logger.log(Level.INFO, "Where clause: " + plainSelect.getWhere().toString());
				
				// 4. visit the expression
				String expressionString = "3 < 5";
				Expression expression = CCJSqlParserUtil.parseCondExpression(expressionString);
				if (expression instanceof BinaryExpression) {
					BinaryExpression binaryExpression = (BinaryExpression) expression;
					deParser = new ExpressionDeParser();
					// boolean result = (Boolean) binaryExpression.accept(deParser);
				}
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}

}
