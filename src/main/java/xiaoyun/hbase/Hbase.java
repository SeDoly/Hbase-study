package xiaoyun.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by xiaoyunxiao on 15-4-2.
 */
public class Hbase {
    private static Configuration conf = HBaseConfiguration.create();

    public static void createTable(String tableName, String[] familys) {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            if (admin.tableExists(tableName)) {
                System.out.println("Table " + tableName + " already exists!");
            } else {
                HTableDescriptor tableDes = new HTableDescriptor(tableName);
                for (String family : familys) {
                    tableDes.addFamily(new HColumnDescriptor(family));
                }
                admin.createTable(tableDes);
                System.out.println("Create table " + tableName + " success!");
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void insertData(String tableName, String rowKey, String family, String column, String value) {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
            System.out.println("Insert data:" + value + " in " + rowKey + ":" + family + ":" + column + " success!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteRow(String tableName, String rowKey) {
        try {
            HTable table = new HTable(conf, tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("Delete row " + rowKey + " success!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteColumn(String tableName, String rowKey, String family, String column) {
        try {
            HTable table = new HTable(conf, tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.deleteColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            table.delete(delete);
            System.out.println("Delete " + rowKey + ":" + family + ":" + column + " success!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void appendData(String tableName, String rowKey, String family, String column, String value) {
        try {
            HTable table = new HTable(conf, tableName);
            Append append = new Append(Bytes.toBytes(rowKey));
            append.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
            table.append(append);
            System.out.println("Append Data " + rowKey + ":" + family + ":" + value + " success!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void incrementData(String tableName, String rowKey, String family, String column, long amount) {
        try {
            HTable table = new HTable(conf, tableName);
            Increment increment = new Increment(Bytes.toBytes(rowKey));
            increment.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), amount);
            table.increment(increment);
            System.out.println("Increment data in " + rowKey + ":" + family + ":" + column + " success!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void getOneRow(String tableName, String rowKey) {
        try {
            HTable table = new HTable(conf, rowKey);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            System.out.println("Row " + rowKey + " begin");
            for (KeyValue kv : result.raw()) {
                System.out.println("Row:" + new String(kv.getRow()));
                System.out.println("Family:" + new String(kv.getFamily()));
                System.out.println("Column:" + new String(kv.getQualifier()));
                System.out.println("Timestamp:" + kv.getTimestamp());
                System.out.println("Value:" + new String(kv.getValue()));
                System.out.println();
            }
            System.out.println("Row " + rowKey + " end");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void scanRows(String tableName, String startRow, String endRow) {
        try {
            HTable table = new HTable(conf, tableName);
            Scan scan = new Scan(startRow.getBytes(), endRow.getBytes());
            ResultScanner scanner = table.getScanner(scan);
            System.out.println("Scan result from " + startRow + " to " + endRow + " begin");
            for (Result result : scanner) {
                for (KeyValue kv : result.raw()) {
                    System.out.println("Row:" + new String(kv.getRow()));
                    System.out.println("Family:" + new String(kv.getFamily()));
                    System.out.println("Column:" + new String(kv.getQualifier()));
                    System.out.println("Timestamp:" + kv.getTimestamp());
                    System.out.println("Value:" + new String(kv.getValue()));
                    System.out.println();
                }
            }
            System.out.println("Scan result from " + startRow + " to " + endRow + " end");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void scanByFilter(String tableName, String family, String column, String value) {
        try {
            HTable table = new HTable(conf, tableName);
            Scan scan = new Scan();
            scan.addColumn(family.getBytes(), column.getBytes());
            Filter filter = new SingleColumnValueFilter(family.getBytes(), column.getBytes(),
                    CompareFilter.CompareOp.EQUAL, value.getBytes());
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println("rowKey:" + new String(result.getRow()));
                for (KeyValue kv : result.raw()) {
                    System.out.println("Family:" + new String(kv.getFamily()));
                    System.out.println("Column:" + new String(kv.getQualifier()));
                    System.out.println("Value:" + new String(kv.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
