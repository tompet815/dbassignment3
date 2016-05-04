package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.query.*;
import java.util.*;

public class NoDupsHashPlan implements Plan {

    private Plan p;
    private Collection<String> sortfields;
    private Schema sch = new Schema();

    public NoDupsHashPlan(Plan p, Collection<String> sortfields, Transaction tx) {
        List<String> sortlist = new ArrayList<String>();
        sortlist.addAll(sortfields);
        this.p = new NDHashPlan(p, sortlist, tx);
        this.sortfields = sortfields;
        for (String fldname : sortfields) {
            sch.add(fldname, p.schema());//add fields 

        }
    }

    public Scan open() {
        Scan s = p.open();
        return new NoDupsHashScan(s, sortfields);
    }

    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    public int recordsOutput() {
        int numgroups = 1;
        for (String fldname : sortfields) {
            numgroups *= p.distinctValues(fldname);
        }
        return numgroups;
    }

    public int distinctValues(String fldname) {
        if (p.schema().hasField(fldname)) {
            return p.distinctValues(fldname);
        }
        else {
            return recordsOutput();
        }
    }

    public Schema schema() {
        return sch;
    }
}
