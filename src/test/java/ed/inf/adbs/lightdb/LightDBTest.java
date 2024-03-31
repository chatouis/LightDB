package ed.inf.adbs.lightdb;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

import java.io.IOException;
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
	}

	@Test
	public void TestEvaluateCondition() throws Exception {
		Map<String, Boolean> testData = new TreeMap<>();
		testData.put("1 < 5", true);
		testData.put("10 < 5", false);
		testData.put("10 > 5", true);
		testData.put("1 > 5", false);
		testData.put("10 = 10", true);
		testData.put("1 < 5 AND 10 > 5 ", true);
		testData.put("1 < 5 OR 10 < 5 ", true);
		testData.put("10 < 5 OR 1 < 5 ", true);
		testData.put("1 > 5 OR 10 < 5 ", false);
		for (Map.Entry<String, Boolean> entry : testData.entrySet()) {
			BinaryExpression binaryExpression = (BinaryExpression)CCJSqlParserUtil.parseCondExpression(entry.getKey());
			EvaluateCondition deparser = new EvaluateCondition(binaryExpression);
			deparser.setBuffer(new StringBuilder());
			binaryExpression.accept(deparser);
			assertEquals(entry.getValue(), deparser.value);
		}
	}
}
