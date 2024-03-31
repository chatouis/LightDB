package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
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
import java.lang.reflect.Array;

/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {

	public static void main(String[] args) {
		Utils.logger.setLevel(Level.INFO);


		String databaseDir = "samples/db";
		String inputSQL = "samples/input/query2.sql";
		String outputFile = "samples/output/query1.csv";

		// if (args.length != 3) {
		// 	System.err.println("Usage: LightDB database_dir input_file output_file");
		// 	return;
		// }
		// String databaseDir = args[0];
		// String inputSQL = args[1];
		// String outputFile = args[2];

		// parsingExample(inputSQL);

		QueryPlan queryPlan = new QueryPlan(inputSQL, databaseDir);
		queryPlan.execute();

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
				
				Utils.logger.log(Level.INFO, "----------------------------------");

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
