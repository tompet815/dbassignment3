package simpledb.materialize;

import simpledb.query.*;
import java.util.*;

public class NoDupsHashScan implements Scan {

    private Scan s;
    private Collection<String> sortfields;
    private GroupValue groupval;
    private boolean moregroups;

    public NoDupsHashScan(Scan s, Collection<String> sortfields) {
        this.s = s;
        this.sortfields = sortfields;
        beforeFirst();
    }

    public void beforeFirst() {
        s.beforeFirst();
        moregroups = s.next();
    }

    public boolean next() {
        if (!moregroups) {
            return false;
        }

        groupval = new GroupValue(s, sortfields);
        while (moregroups = s.next()) {
            GroupValue gv = new GroupValue(s, sortfields);
            //System.out.println("Groupval hash: "+gv.hashCode()+gv.getVal("name")+gv.getVal("age"));
            if (!groupval.equals(gv)) {
                break;
            }
        }
        return true;
    }

    public void close() {
        s.close();
    }

    public Constant getVal(String fldname) {
        if (sortfields.contains(fldname)) {
            return groupval.getVal(fldname);
        }

        throw new RuntimeException("field " + fldname + " not found.");
    }

    public int getInt(String fldname) {
        return (Integer) getVal(fldname).asJavaVal();
    }

    public String getString(String fldname) {
        return (String) getVal(fldname).asJavaVal();
    }

    public boolean hasField(String fldname) {
        if (sortfields.contains(fldname)) {
            return true;
        }

        return false;
    }
}
