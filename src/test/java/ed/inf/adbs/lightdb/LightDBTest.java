package ed.inf.adbs.lightdb;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Unit tests for LightDB.
 */
public class LightDBTest {
	
	@Test
	public void TestScanOperator() throws IOException {
		ScanOperator scanOperator = new ScanOperator("samples/db/data/Boats.csv");
		assertEquals("101, 2, 3", scanOperator.getNextTuple());
		assertEquals("102, 3, 4", scanOperator.getNextTuple());
		assertEquals("104, 104, 2", scanOperator.getNextTuple());
		scanOperator.reset();
		assertEquals("101, 2, 3", scanOperator.getNextTuple());
		assertEquals("102, 3, 4", scanOperator.getNextTuple());
		assertEquals("104, 104, 2", scanOperator.getNextTuple());
		assertEquals("103, 1, 1", scanOperator.getNextTuple());
		assertEquals("107, 2, 8", scanOperator.getNextTuple());
		assertEquals(null, scanOperator.getNextTuple());
	}

	@Test
	public void TestEvaluateConditionWithColumn4() throws Exception {
		String[] _schema = {"A", "B", "C"};
		List<String> schema = Arrays.asList(_schema);

		String query = "SELECT * FROM table WHERE table.A = table.C AND table.B > 4";
		PlainSelect plainSelect = (PlainSelect) CCJSqlParserUtil.parse(query);
		BinaryExpression binaryExpression = (BinaryExpression) plainSelect.getWhere();

		Map<String, Boolean> testData = new TreeMap<>();
		testData.put("1,2,1", false);
		testData.put("2,3,4", false);
		testData.put("2,4,5", false);
		testData.put("1,5,1", true);

		for (Map.Entry<String, Boolean> entry : testData.entrySet()) {
			EvaluateCondition deparser = new EvaluateCondition(schema, binaryExpression, entry.getKey());
			deparser.setBuffer(new StringBuilder());
			binaryExpression.accept(deparser);
			assertEquals(entry.getValue(), deparser.value);
		}

	}

	@Test
	public void TestEvaluateConditionWithColumn10() throws Exception {
		String[] _schema = {"A", "B", "C"};
		List<String> schema = Arrays.asList(_schema);

		String query = "SELECT * FROM table WHERE table.A = 2 AND table.B < tabel.C";
		PlainSelect plainSelect = (PlainSelect) CCJSqlParserUtil.parse(query);
		BinaryExpression binaryExpression = (BinaryExpression) plainSelect.getWhere();

		// List<String> tuples = Arrays.asList("1,2,3", "2,3,4", "3,4,5"); 
		Map<String, Boolean> testData = new TreeMap<>();
		testData.put("1,2,3", false);
		testData.put("2,3,4", true);
		testData.put("2,4,5", true);

		for (Map.Entry<String, Boolean> entry : testData.entrySet()) {
			EvaluateCondition deparser = new EvaluateCondition(schema, binaryExpression, entry.getKey());
			deparser.setBuffer(new StringBuilder());
			binaryExpression.accept(deparser);
			assertEquals(entry.getValue(), deparser.value);
		}
	}

	@Test
	public void TestSelectOperator() throws IOException, JSQLParserException {
		String query = "SELECT * FROM Boats WHERE Boats.E = 2";
		Statement statement = CCJSqlParserUtil.parse(query);
		PlainSelect plainSelect = (PlainSelect) statement;

		String databaseDir = "samples/db";
		Map<String, List<String>> schema = Utils.loadSchema(databaseDir + "/schema.txt");

		SelectOperator selectOperator = new SelectOperator(
			"samples/db/data/Boats.csv", plainSelect.getWhere(), schema.get("Boats"));
		assertEquals("101, 2, 3", selectOperator.getNextTuple());
		assertEquals("107, 2, 8", selectOperator.getNextTuple());
		assertEquals(null, selectOperator.getNextTuple());
	}


	@Test
	public void TestProjectSelectOperator() throws Exception {
		String query = "SELECT Boats.E, Boats.F FROM Boats WHERE Boats.E = 2";
		Statement statement = CCJSqlParserUtil.parse(query);
		PlainSelect plainSelect = (PlainSelect) statement;

		String databaseDir = "samples/db";
		Map<String, List<String>> schema = Utils.loadSchema(databaseDir + "/schema.txt");

		ProjectSelectOperator projectSelectOperator = new ProjectSelectOperator(
			"samples/db/data/Boats.csv", plainSelect.getWhere(), schema.get("Boats"), plainSelect.getSelectItems());
		assertEquals("2, 3", projectSelectOperator.getNextTuple());
		assertEquals("2, 8", projectSelectOperator.getNextTuple());
		assertEquals(null, projectSelectOperator.getNextTuple());
	}

	@Test
	public void TestEvaluateConditionJoin() throws Exception {
		Map<String, List<String>> schema = new TreeMap<>();
		schema.put("table1", Arrays.asList("A", "B", "C"));
		schema.put("table2", Arrays.asList("G", "H", "I"));

		String query = "SELECT * FROM table1, table2 WHERE table1.A = table2.G AND table1.B = 4";
		PlainSelect plainSelect = (PlainSelect) CCJSqlParserUtil.parse(query);
		BinaryExpression binaryExpression = (BinaryExpression) plainSelect.getWhere();

		Map<List<String>, Boolean> testData = new HashMap<>();
		List<String> tuplePair1 = Arrays.asList("1,2,3", "1,2,3");
		List<String> tuplePair2 = Arrays.asList("3,3,4", "2,3,4");
		List<String> tuplePair3 = Arrays.asList("2,4,5", "2,4,5");
		testData.put(tuplePair1, false);
		testData.put(tuplePair2, false);
		testData.put(tuplePair3, true);

		for (Map.Entry<List<String>, Boolean> entry : testData.entrySet()) {
			EvaluateConditionJoin deparser = new EvaluateConditionJoin(schema, binaryExpression, entry.getKey().get(0), entry.getKey().get(1));
			deparser.setBuffer(new StringBuilder());
			binaryExpression.accept(deparser);
			assertEquals(entry.getValue(), deparser.value);
		}
	}

	@Test
	public void TestJoinOperator1() throws Exception {
		String query = "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G AND Sailors.B = 100";

		Statement statement = CCJSqlParserUtil.parse(query);
		PlainSelect plainSelect = (PlainSelect) statement;

		String databaseDir = "samples/db";
		Map<String, List<String>> schema = Utils.loadSchema(databaseDir + "/schema.txt");

		JoinOperator joinOperator = new JoinOperator(
			"samples/db/data/Sailors.csv", "samples/db/data/Reserves.csv", plainSelect.getWhere(), schema);
		assertEquals("3, 100, 105, 3, 102", joinOperator.getNextTuple());
		assertEquals("4, 100, 50, 4, 104", joinOperator.getNextTuple());
		assertEquals(null, joinOperator.getNextTuple());
	}

	@Test
	public void TestJoinOperator2() throws Exception {
		String query = "SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G AND Sailors.B = 200";

		Statement statement = CCJSqlParserUtil.parse(query);
		PlainSelect plainSelect = (PlainSelect) statement;

		String databaseDir = "samples/db";
		Map<String, List<String>> schema = Utils.loadSchema(databaseDir + "/schema.txt");

		JoinOperator joinOperator = new JoinOperator(
			"samples/db/data/Sailors.csv", "samples/db/data/Reserves.csv", plainSelect.getWhere(), schema);
		assertEquals("1, 200, 50, 1, 101", joinOperator.getNextTuple());
		assertEquals("1, 200, 50, 1, 102", joinOperator.getNextTuple());
		assertEquals("1, 200, 50, 1, 103", joinOperator.getNextTuple());
		assertEquals("2, 200, 200, 2, 101", joinOperator.getNextTuple());
		assertEquals(null, joinOperator.getNextTuple());
	}
}
