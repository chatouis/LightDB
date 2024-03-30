package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ScanOperator extends Operator{
    private BufferedReader reader;
    private static String inputFile;

    public ScanOperator(String inputFile) throws IOException{
        reader = new BufferedReader(new FileReader(inputFile));
    }
    
    @Override
    public String getNextTuple() throws IOException{
        String line = reader.readLine();
        if (line == null) {
            return null;
        }
        return line;
    }

    @Override
    public void reset() throws IOException {
        reader.close();
        reader = new BufferedReader(new FileReader(inputFile));
    }
}
