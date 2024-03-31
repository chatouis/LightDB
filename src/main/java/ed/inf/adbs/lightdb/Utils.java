package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Utils {
    private static Utils utils = new Utils();

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

}
