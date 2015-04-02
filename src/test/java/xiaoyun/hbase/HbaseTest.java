package xiaoyun.hbase;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HbaseTest extends TestCase {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testCreateTable() throws Exception {
        String tableName = "xiaoyun";
        String[] family = {"game"};
        Hbase.createTable(tableName, family);
    }

    @Test
    public void testInsertData() throws Exception {
        Hbase.insertData("xiaoyun","first","game","civ","boring1");
    }

    @Test
    public void testDeleteRow() throws Exception {

    }

    @Test
    public void testDeleteColumn() throws Exception {

    }

    @Test
    public void testAppendData() throws Exception {

    }

    @Test
    public void testIncrementData() throws Exception {

    }

    @Test
    public void testGetOneRow() throws Exception {
//        Hbase.getOneRow();
    }

    @Test
    public void testScanRows() throws Exception {

    }

    @Test
    public void testScanByFilter() throws Exception {

    }
}