package ed.inf.adbs.lightdb;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public abstract class Operator {
    private boolean distinct;
    private Set<String> distinctSet;

    public abstract String getNextTuple() throws IOException;
    
    public abstract void reset() throws IOException ;
    
    public void dump() throws IOException {
        String tuple;
        while ((tuple = getNextTuple()) != null) {
            if (distinct == false ) {
                System.out.println(tuple);
            }
            else if (distinctSet.contains(tuple) == false) {
                System.out.println(tuple);
                distinctSet.add(tuple);
            }
        }
    }
    
    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
        if (distinct) {
            distinctSet = new HashSet<>();
        } else {
            distinctSet = null;
        }
    }
}