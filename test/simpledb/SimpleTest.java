/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb;

import static java.sql.Types.*;
import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import simpledb.index.hash.HashIndex;
import simpledb.metadata.TableMgr;
import simpledb.query.BoolConstant;
import simpledb.query.FloatConstant;
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
public class SimpleTest {

    static Transaction tx;
    static TableScan scan;
    static final String tableName = "mytable";
    static final String dbName = "mytestdb";
    static HashIndex hi;
    static TableInfo tableInfo;

    @BeforeClass
    public static void setupClass() throws IOException {
        SimpleDB.dropDatabase(dbName);
        SimpleDB.init(dbName);
        tx = new Transaction();

        Schema schema = new Schema();

        schema.addStringField("name", TableMgr.MAX_NAME);
        schema.addIntField("age");
        schema.addField("bool", BOOLEAN, 1);
        schema.addField("float", FLOAT, 10);
        SimpleDB.mdMgr().createTable(tableName, schema, tx);

        tableInfo= SimpleDB.mdMgr().getTableInfo(tableName, tx);
        RecordFile file = new RecordFile(tableInfo, tx);
        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
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
    public void testBooleanValue() {
        tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        scan = new TableScan(tableInfo, tx);
        scan.beforeFirst();
        scan.next();
        scan.setVal("bool", new BoolConstant(Boolean.TRUE));
        BoolConstant boo = (BoolConstant) scan.getVal("bool");
        assertTrue(boo.asJavaVal());
        scan.next();
        scan.setVal("bool", new BoolConstant(Boolean.FALSE));
        boo = (BoolConstant) scan.getVal("bool");
        assertFalse(boo.asJavaVal());

    }

    @Test
    public void testFloatValue() {
        tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        scan = new TableScan(tableInfo, tx);
        scan.beforeFirst();
        scan.next();
        Float floatvalue = 1.2345f;
        scan.setVal("float", new FloatConstant(floatvalue));
        FloatConstant flo = (FloatConstant) scan.getVal("float");
        assertEquals(floatvalue, flo.asJavaVal());

    }

}
