/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import simpledb.index.hash.HashIndex;
import simpledb.materialize.NoDupsHashPlan;
import simpledb.materialize.NoDupsHashScan;
import simpledb.materialize.NoDupsSortPlan;
import simpledb.materialize.NoDupsSortScan;
import simpledb.metadata.TableMgr;
import simpledb.query.TablePlan;
import simpledb.query.TableScan;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 *
 * @author Tomoe
 */
public class SimpleTestDuplicateElimination {

    static Transaction tx;
    static TableScan scan;
    static TableInfo tableInfo;
    static final String tableName = "mytable";
    static final String dbName = "mytestdb";
    static HashIndex hi;

    @BeforeClass
    public static void setupClass() throws IOException {
        SimpleDB.dropDatabase(dbName);
        SimpleDB.init(dbName);
        tx = new Transaction();

        Schema schema = new Schema();

        schema.addStringField("name", TableMgr.MAX_NAME);
        schema.addIntField("age");
        SimpleDB.mdMgr().createTable(tableName, schema, tx);

        tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        RecordFile file = new RecordFile(tableInfo, tx);
        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 25);
        file.insert();
        file.setString("name", "John");
        file.setInt("age", 25);
        file.insert();
        file.setString("name", "Ellen");
        file.setInt("age", 25);
        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
        file.insert();
        file.setString("name", "John");
        file.setInt("age", 23);
        file.insert();
        file.setString("name", "John");
        file.setInt("age", 23);
        file.insert();
        file.setString("name", "John");
        file.setInt("age", 25);
        file.insert();
        file.setString("name", "John");
        file.setInt("age", 23);
        file.insert();
        file.setString("name", "Ellen");
        file.setInt("age", 25);
        tx.commit();

    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        SimpleDB.dropDatabase(dbName);

    }

    @After
    public void tearDown() {
        tx.commit();
    }

    @Test
    public void testNoDuplicateSort() {
        System.out.println("----------------- original ---------------------");
        tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        scan = new TableScan(tableInfo, tx);
        while (scan.next()) {
            System.out.println("Age: " + scan.getVal("age"));
            System.out.println("Name: " + scan.getVal("name"));
        }

        Collection<String> list = new ArrayList();
        list.add("age");
        list.add("name");
        TablePlan plan = new TablePlan(tableName, tx);
        NoDupsSortPlan ndsp = new NoDupsSortPlan(plan, list, tx);
        NoDupsSortScan gbs = (NoDupsSortScan) ndsp.open();
        gbs.beforeFirst();
        System.out.println("");
        System.out.println("-----------------after sort---------------------");
        while (gbs.next()) {
            System.out.println("Age: " + gbs.getVal("age"));
            System.out.println("Name: " + gbs.getVal("name"));
        }

    }

    @Test
    public void testNoDuplicateHash() {

        Collection<String> list = new ArrayList();
        list.add("age");
        list.add("name");
        TablePlan plan = new TablePlan(tableName, tx);
        NoDupsHashPlan ndsp = new NoDupsHashPlan(plan, list, tx);
        NoDupsHashScan gbs = (NoDupsHashScan) ndsp.open();
        gbs.beforeFirst();
        System.out.println("");
        System.out.println("-----------------after hashing---------------------");
        while (gbs.next()) {
            System.out.println("Age: " + gbs.getVal("age"));
            System.out.println("Name: " + gbs.getVal("name"));
        }

    }
}
