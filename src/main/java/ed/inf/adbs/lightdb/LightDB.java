package ed.inf.adbs.lightdb;

import java.util.logging.*;


/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {

	public static void main(String[] args) {
		Utils.logger.setLevel(Level.WARNING);


		String databaseDir = "samples/db";
		String inputSQL = "samples/input/query7.sql";
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



}
