package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.*;
import java.util.stream.Collectors;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class Utils {
    private static Utils utils = new Utils();
	public static Logger logger = Logger.getLogger(LightDB.class.getName());

    private Utils() {}

    public static Utils getInstance() {
        return utils;
    }

    public String getTablePath(String tableName) {
        return "samples/db/data/" + tableName + ".csv";
    }

	public static Map<String, List<String>> loadSchema(String schemaFile) throws IOException{
		Map<String, List<String>> schema = new TreeMap<String, List<String>>();
		BufferedReader reader = new BufferedReader(new FileReader(schemaFile));
		String tuple;
		while ((tuple = reader.readLine()) != null) {
			List<String> parts = Arrays.asList(tuple.split(" "));
			schema.put(parts.get(0), parts.subList(1, parts.size()));
		}
		reader.close();
		return schema;
	}


	public static FromItem parsingFromItem(String queryFile) throws FileNotFoundException, JSQLParserException {
		Statement statement = CCJSqlParserUtil.parse(new FileReader(queryFile));
		if (statement != null) {
			PlainSelect plainSelect = (PlainSelect) statement;
			FromItem from = plainSelect.getFromItem();
			return from;

		}
		return null;
	}

	public static List<Join> parsingJoins(String queryFile) throws FileNotFoundException, JSQLParserException {
		Statement statement = CCJSqlParserUtil.parse(new FileReader(queryFile));
		if (statement != null) {
			PlainSelect plainSelect = (PlainSelect) statement;
			return plainSelect.getJoins();
		}
		return null;
	}

	public static Set<String> parsingTablesSet(String queryFile) throws FileNotFoundException, JSQLParserException {
		Statement statement = CCJSqlParserUtil.parse(new FileReader(queryFile));
		if (statement != null) {
			TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
			return tablesNamesFinder.getTables(statement);
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

	public static List<SelectItem<?>> parsingSelectItems(String queryFile) throws FileNotFoundException, JSQLParserException {
		Statement statement = CCJSqlParserUtil.parse(new FileReader(queryFile));
		if (statement != null) {
			PlainSelect plainSelect = (PlainSelect) statement;
			return plainSelect.getSelectItems();
		}
		return null;
	}

	public static List<String> parsingAliases(String queryFile) throws Exception {
		Statement statement = CCJSqlParserUtil.parse(new FileReader(queryFile));
		Boolean existAlias = false;
		List<String> aliases = new ArrayList<String>();
		if (statement != null) {
			PlainSelect plainSelect = (PlainSelect) statement;
			FromItem fromItem = plainSelect.getFromItem();
			String alias = fromItem.toString().split(" ").length == 2 ? fromItem.toString().split(" ")[1] : "";
			aliases.add(alias);
			existAlias = fromItem.toString().split(" ").length == 2 ? true : false;
			if (plainSelect.getJoins() != null) {
				for (Join join : plainSelect.getJoins()) {
					alias = join.toString().split(" ").length == 2 ? join.toString().split(" ")[1] : "";
					aliases.add(alias);
					existAlias = fromItem.toString().split(" ").length == 2 ? true : false;
				}
			}
		}
		if (existAlias) {
			return aliases;
		}
		else {
			return null;
		}
	}

	//  Parsing example
	public static void parsingExample(String filename) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(filename));
			if (statement != null) {
				PlainSelect plainSelect = (PlainSelect) statement;
				Utils.logger.log(Level.INFO, "Read statement: " + statement);

				// 1. directly parse the SQL query
				Utils.logger.log(Level.INFO, "the first table: " + plainSelect.getFromItem());	
				Utils.logger.log(Level.INFO, "other tables: " + plainSelect.getJoins());
				Utils.logger.log(Level.INFO, "Where:" + plainSelect.getWhere());
				Utils.logger.log(Level.INFO, "Distinct:  " + plainSelect.getDistinct());
				Utils.logger.log(Level.INFO, "OrderByElements: " + plainSelect.getOrderByElements());
				Utils.logger.log(Level.INFO, "GroupByElements: " + plainSelect.getGroupBy());
				Utils.logger.log(Level.INFO, "SelectItems: " + plainSelect.getSelectItems());
				
				Utils.logger.log(Level.INFO, "-----s-----------------------------");

				// 2. use expression to extract info
				Table table = (Table) plainSelect.getFromItem();
				Utils.logger.log(Level.INFO, "Table name: " + table.getName());
				if (plainSelect.getWhere() instanceof EqualsTo) {
					EqualsTo equalsTo = (EqualsTo) plainSelect.getWhere();
					Column a = (Column) equalsTo.getLeftExpression();
					Utils.logger.log(Level.INFO, "Left expression: " + a);
					Column b = (Column) equalsTo.getRightExpression();
					Utils.logger.log(Level.INFO, "Right expression: " + b);
				}
				Utils.logger.log(Level.INFO, "----------------------------------");

				// 3. use net.sf.jsqlparser.util to extract table names, where clause, etc.
				TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
				Utils.logger.log(Level.INFO, "Table names: " + tablesNamesFinder.getTables(statement));		

				ExpressionDeParser deParser = new ExpressionDeParser();
				Expression where = plainSelect.getWhere();
				where.accept(deParser);
				Utils.logger.log(Level.INFO, "Where clause: " + deParser.getBuffer().toString());

				plainSelect.getWhere().accept(new ExpressionVisitorAdapter() {
					@Override
					public void visit(Column column) {
						column.setColumnName(column.getColumnName().replace("_", ""));
					}
				});
				Utils.logger.log(Level.INFO, "Where clause: " + plainSelect.getWhere().toString());
				
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}
