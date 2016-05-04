package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.*;

public class NDSortPlan implements Plan {

    private Plan p;
    private Transaction tx;
    private Schema sch;
    private Schema tempsch = new Schema();
    private RecordComparator comp;

    public NDSortPlan(Plan p, List<String> sortfields, Transaction tx) {
        this.p = p;
        this.tx = tx;
        sch = p.schema();
        comp = new RecordComparator(sortfields);
    }

    public Scan open() {
        Scan src = p.open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();
        while (runs.size() > 2) {
            runs = doAMergeIteration(runs);
        }
        return new SortScan(runs, comp);
    }

    public int blocksAccessed() {
        // does not include the one-time cost of sorting
        Plan mp = new MaterializePlan(p, tx); // not opened; just for analysis
        return mp.blocksAccessed();
    }

    public int recordsOutput() {
        return p.recordsOutput();
    }

    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    public Schema schema() {
        return sch;
    }

    private List<TempTable> splitIntoRuns(Scan src) {
        List<TempTable> temps = new ArrayList<TempTable>();
        src.beforeFirst();

        if (!src.next()) {
            return temps;
        }
        TempTable currenttemp = new TempTable(sch, tx);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();

        while (copy(src, currentscan)) {
            if (comp.compare(src, currentscan) <= 0) {
                if (comp.compare(src, currentscan) == 0) {
                    currentscan.delete();
                }
                // start a new run
                currentscan.close();
                currenttemp = new TempTable(sch, tx);
                temps.add(currenttemp);
                currentscan = (UpdateScan) currenttemp.open();
            }
        }
        currentscan.close();
        return temps;
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<TempTable>();
        while (runs.size() > 1) {
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoRuns(p1, p2));
        }
        if (runs.size() == 1) {
            result.add(runs.get(0));
        }
        return result;
    }

    private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(sch, tx);
        UpdateScan dest = result.open();

        boolean hasmore1 = src1.next();
        boolean hasmore2 = src2.next();
        while (hasmore1 && hasmore2) {
            if (comp.compare(src1, src2) < 0) {
                hasmore1 = copy(src1, dest);
            }
            else {

                hasmore2 = copy(src2, dest);
            }
        }

        if (hasmore1) {
            while (hasmore1) {
                hasmore1 = copy(src1, dest);
            }
        }
        else {
            while (hasmore2) {
                hasmore2 = copy(src2, dest);
            }
        }
        src1.close();
        src2.close();
        dest.close();
        return result;
    }

    private boolean copy(Scan src, UpdateScan dest) {
        dest.insert();

        for (String fldname : sch.fields()) {
            dest.setVal(fldname, src.getVal(fldname));

        }

        return src.next();
    }
}
