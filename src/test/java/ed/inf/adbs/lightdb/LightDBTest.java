package ed.inf.adbs.lightdb;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import org.junit.Test;

import java.io.IOException;

import ed.inf.adbs.lightdb.ScanOperator; 

/**
 * Unit tests for LightDB.
 */
public class LightDBTest {
	
	/**
	 * Rigorous Test :-)
	 * @throws IOException 
	 */
	@Test
	public void TestScanOperator() throws IOException {
		ScanOperator scanOperator = new ScanOperator("samples/db/data/Boats.csv");
		assertEquals("101, 2, 3", scanOperator.getNextTuple());
		assertEquals("102, 3, 4", scanOperator.getNextTuple());
		assertEquals("104, 104, 2", scanOperator.getNextTuple());
		scanOperator.reset();
		assertEquals("101, 2, 3", scanOperator.getNextTuple());
	}
}
