package simpledb.materialize;

import simpledb.record.RID;
import simpledb.query.*;
import java.util.*;

public class HashScan implements Scan {

    private UpdateScan s1, s2 = null, currentscan = null;
    private RecordComparator comp;
    private boolean hasmore1, hasmore2 = false;
    private List<RID> savedposition;

    public HashScan(List<TempTable> runs, RecordComparator comp) {
        this.comp = comp;
        s1 = (UpdateScan) runs.get(0).open();
        hasmore1 = s1.next();
        if (runs.size() > 1) {
            s2 = (UpdateScan) runs.get(1).open();
            hasmore2 = s2.next();
        }
    }

    public void beforeFirst() {
        currentscan = null;
        s1.beforeFirst();
        hasmore1 = s1.next();
        if (s2 != null) {
            s2.beforeFirst();
            hasmore2 = s2.next();
        }
    }

    public boolean next() {
        if (currentscan != null) {
            if (currentscan == s1) {
                hasmore1 = s1.next();
            }
            else if (currentscan == s2) {
                hasmore2 = s2.next();
            }
        }

        if (!hasmore1 && !hasmore2) {
            return false;
        }
        else if (hasmore1 && hasmore2) {
            if (comp.compare(s1, s2) < 0) {
                currentscan = s1;
            }
            else {
                currentscan = s2;
            }
        }
        else if (hasmore1) {
            currentscan = s1;
        }
        else if (hasmore2) {
            currentscan = s2;
        }
        return true;
    }

    public void close() {
        s1.close();
        if (s2 != null) {
            s2.close();
        }
    }

    public Constant getVal(String fldname) {
        return currentscan.getVal(fldname);
    }

    public int getInt(String fldname) {
        return currentscan.getInt(fldname);
    }

    public String getString(String fldname) {
        return currentscan.getString(fldname);
    }

    public boolean hasField(String fldname) {
        return currentscan.hasField(fldname);
    }

    public void savePosition() {
        RID rid1 = s1.getRid();
        RID rid2 = (s2 == null) ? null : s2.getRid();
        savedposition = Arrays.asList(rid1, rid2);
    }

    public void restorePosition() {
        RID rid1 = savedposition.get(0);
        RID rid2 = savedposition.get(1);
        s1.moveToRid(rid1);
        if (rid2 != null) {
            s2.moveToRid(rid2);
        }
    }
}
