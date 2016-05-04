package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;

import java.util.*;

public class NDHashPlan implements Plan {

    private Plan p;
    private Transaction tx;
    private Schema sch;
    private RecordComparator comp;
    private List<String> fields = new ArrayList();

    public NDHashPlan(Plan p, List<String> sortfields, Transaction tx) {
        this.p = p;
        this.tx = tx;
        sch = p.schema();
        this.fields.addAll(sortfields);
        comp = new RecordComparator(sortfields);
    }

    public Scan open() {
        Scan src = p.open();

        List<TempTable> runs = split(src);
        src.close();

        while (runs.size() > 2) {
            runs = doAMergeIteration(runs);
        }
        return new HashScan(runs, comp);
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

    private List<TempTable> split(Scan src) {
        src.beforeFirst();
        HashMap<Integer, List<TempTable>> map = new HashMap();
        while (src.next()) {
            TempTable currenttemp = new TempTable(sch, tx);
            UpdateScan currentscan = currenttemp.open();
            int hash = new GroupValue(src, fields).hashCode();
            copyvoid(src, currentscan);
            if (map.get(hash) == null) {
                List<TempTable> temp = new ArrayList();
                temp.add(currenttemp);
                map.put(hash, temp);
            }
            else {
                map.get(hash).add(currenttemp);
            }
            currentscan.close();
        }

        return createOneList(map);

    }

    private List<TempTable> createOneList(HashMap<Integer, List<TempTable>> map) {

        List<TempTable> resList = new ArrayList();

        HashMap<TempTable, TempTable> tempmap = new HashMap();
        for (Map.Entry<Integer, List<TempTable>> entrySet : map.entrySet()) {

            List<TempTable> tables = entrySet.getValue();
            if (tables.size() > 1) {

                for (Iterator<TempTable> it = tables.iterator(); it.hasNext();) {
                    TempTable tt = it.next();
                    UpdateScan s1 = tt.open();
                    s1.beforeFirst();
                    s1.next();
                    GroupValue gv = new GroupValue(s1, fields);
                    tempmap.put(tt, tt);
                    s1.close();
                }
                for (Map.Entry<TempTable, TempTable> entrySet1 : tempmap.entrySet()) {

                    TempTable value = entrySet1.getValue();
                    resList.add(value);

                }
            }
            else {
                resList.add(tables.get(0));
            }

        }
        return resList;
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

    private void copyvoid(Scan src, UpdateScan dest) {
        dest.insert();
        for (String fldname : sch.fields()) {
            dest.setVal(fldname, src.getVal(fldname));
        }

    }

}
