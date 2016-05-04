/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package simpledb;


import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import simpledb.index.hash.HashIndex;
import simpledb.materialize.HashJoinPlan;
import simpledb.materialize.HashJoinScan;
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
public class SimpleTestHashJoin {

    static Transaction tx;
    static TableScan scan;
    static TableInfo tableInfo;
    static TableInfo tableInfo2;

    static final String tableName = "mytable";
    static final String tableName2 = "mytable2";

    static final String dbName = "mytestdb";
    static HashIndex hi;

    @BeforeClass
    public static void setupClass() throws IOException {
        SimpleDB.dropDatabase(dbName);
        SimpleDB.init(dbName);
        tx = new Transaction();

        Schema schema = new Schema();
        schema.addIntField("id");
        schema.addStringField("name", TableMgr.MAX_NAME);
        schema.addIntField("age");
        SimpleDB.mdMgr().createTable(tableName, schema, tx);
        
        
        Schema schema2 = new Schema();
        schema2.addIntField("id");
        schema2.addStringField("city", TableMgr.MAX_NAME);
        schema2.addIntField("person_id");
        SimpleDB.mdMgr().createTable(tableName2, schema2, tx);

        tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        tableInfo2 = SimpleDB.mdMgr().getTableInfo(tableName2, tx);

        RecordFile file = new RecordFile(tableInfo, tx);
        file.insert();
        file.setInt("id", 1);
        file.setString("name", "Peter");
        file.setInt("age", 23);
        file.insert();
        file.setInt("id", 2);
        file.setString("name", "John");
        file.setInt("age", 25);
        file.insert();
        file.setInt("id", 3);
        file.setString("name", "Ellen");
        file.setInt("age", 25);
        
        RecordFile file2 = new RecordFile(tableInfo2, tx);
        file2.insert();
        file2.setInt("id", 1);
        file2.setString("city", "Osaka");
        file2.setInt("person_id", 3);
        file2.insert();
        file2.setInt("id", 2);
        file2.setString("city", "Copenhagen");
        file2.setInt("person_id", 1);
        file2.insert();
        file2.setInt("id", 3);
        file2.setString("city", "Stockholm");
        file2.setInt("person_id", 2);
        
        
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

        TablePlan p1 = new TablePlan(tableName, tx);
        TablePlan p2 = new TablePlan(tableName2, tx);
        HashJoinPlan hjp = new HashJoinPlan(p1,p2,"id","person_id",tx) ;
        HashJoinScan hjs = (HashJoinScan) hjp.open();
        hjs.beforeFirst();
           System.out.println("");
        System.out.println("-----------------after hashing---------------------");
         System.out.println("| id | person_id | name | city   ");
        while (hjs.next()) {
           
            System.out.println("|  "+ hjs.getVal("id") +" |     "+hjs.getVal("person_id")+"     | "+ hjs.getVal("name")+" | "+hjs.getVal("city"));
        }
  }
}
