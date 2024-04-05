package ed.inf.adbs.lightdb.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ed.inf.adbs.lightdb.ScanOperator;
import ed.inf.adbs.lightdb.TupleComparator;


public class SortScanOperator extends ScanOperator{
    List<String> allTuple;
    List<String> schema;
    String orderBy;

    public SortScanOperator(String inputFile, String orderBy, List<String> schema) throws IOException {
        super(inputFile);
        this.schema = schema;
        this.orderBy = orderBy;
        allTuple = new ArrayList<String>();
    }
    
    @Override
    public String getNextTuple() throws IOException {
        return super.getNextTuple();
    }

    @Override
    public void dump() throws IOException{
        String tuple;
        while ((tuple = getNextTuple()) != null) {
            allTuple.add(tuple);
        }
        Collections.sort(allTuple, new TupleComparator(schema, orderBy));
        for (String t : allTuple) {
            System.out.println(t);
        }
    }
}
