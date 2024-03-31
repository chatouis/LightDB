package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ScanOperator extends Operator{
    private BufferedReader reader;

    public ScanOperator(String inputFile) throws IOException{
        reader = new BufferedReader(new FileReader(inputFile));
        reader.mark(8192); // 8192 is defaultCharBufferSize
    }
    
    @Override
    public String getNextTuple() throws IOException{
        String tuple = reader.readLine();
        if (tuple == null) {
            return null;
        }
        return tuple;
    }

    @Override
    public void reset() throws IOException {
        reader.reset();
    }
}
