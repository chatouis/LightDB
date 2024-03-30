package ed.inf.adbs.lightdb;

import java.io.IOException;

public abstract class Operator {
    public abstract String getNextTuple() throws IOException;
    public abstract void reset() throws IOException ;
    public void dump() throws IOException{
        String tuple;
        while ((tuple = getNextTuple()) != null) {
            System.out.println(tuple);
        }
    }
}