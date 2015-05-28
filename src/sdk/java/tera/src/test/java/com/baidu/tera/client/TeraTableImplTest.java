package com.baidu.tera.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Arrays;

/**
 * Unit test for TeraTableImpl.
 */
public class TeraTableImplTest
{
    private String tableName;
    private TeraClientImpl client;

    public TeraTableImplTest()
    {

    }

    public String getTableName() {
        return "jtable_impl_ut_" + Long.toString(System.currentTimeMillis() % 10000);
    }

    @Before
    public void init() {
        try {
            tableName = getTableName();
            String tableSchema = tableName + "{lg1{cf11,cf12},lg2{cf21<maxversions=3>,cf22,cf23}}";
            client = new TeraClientImpl();
            assertTrue(client.createTable(tableName, tableSchema));
            System.out.println("[Tera Debug] Create table:" + tableName);
        } catch (Exception e) {
            System.out.println("exception occurs in TeraTableImplTest: " + e.toString());
        }
    }

    @Test
    public void testPutGet() {
        System.out.println("[Tera Debug] Now running " + Thread.currentThread().getStackTrace()[1].getMethodName() + " ...");
        try {
            System.out.println("Put 5 records to tera ...");
            TeraTableImpl table = new TeraTableImpl(tableName);
            assertTrue(table.put("table row1", "cf11", "qualifier1", "table value1"));
            assertTrue(table.put("table row1", "cf11", "qualifier2", "table value2"));
            assertTrue(table.put("table row1", "cf21", "qualifier1", "table value3"));
            assertTrue(table.put("table row1", "cf22", "qualifier1", "table value4"));
            assertTrue(table.put("table row1", "cf23", "qualifier1", "table value5"));

            System.out.println("Get 5 records from tera ...");
            String value;
            value = table.get("table row1", "cf11", "qualifier1");
            assertTrue(value.equals("table value1"));
            value = table.get("table row1", "cf11", "qualifier2");
            assertTrue(value.equals("table value2"));
            value = table.get("table row1", "cf21", "qualifier1");
            assertTrue(value.equals("table value3"));
            value = table.get("table row1", "cf22", "qualifier1");
            assertTrue(value.equals("table value4"));
            value = table.get("table row1", "cf23", "qualifier1");
            assertTrue(value.equals("table value5"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMutation() {
        System.out.println("[Tera Debug] Now running " + Thread.currentThread().getStackTrace()[1].getMethodName() + " ...");
        try {
            TeraTableImpl table = new TeraTableImpl(tableName, "");

            System.out.println("Flush mutation to tera ...");
            byte[] rowKey = "mutation1".getBytes();
            TeraMutationImpl mutation = table.newMutation(rowKey);
            assertTrue(mutation.add("cf11".getBytes(), "column1".getBytes(), "value1".getBytes()));
            assertTrue(mutation.add("cf11".getBytes(), "column1".getBytes(), "value2".getBytes()));
            assertTrue(mutation.add("cf11".getBytes(), "column2".getBytes(), "value3".getBytes()));
            assertTrue(mutation.add("cf12".getBytes(), "column3".getBytes(), "value4".getBytes()));
            assertTrue(mutation.deleteColumn("cf11".getBytes(), "column2".getBytes()));
            assertTrue(mutation.deleteColumns("cf11".getBytes(), "column1".getBytes()));
            assertTrue(mutation.deleteFamily("cf12".getBytes()));
            table.applyMutation(mutation);
            table.flushCommits();

            System.out.println("Read records from tera ...");
            TeraReaderImpl reader = table.newReader(rowKey);
            assertTrue(reader.add("cf11".getBytes(), null));
            assertTrue(reader.add("cf12".getBytes(), null));

            TeraResultImpl result = table.applyReader(reader);
            assertTrue(result.done());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testReader() {
        System.out.println("[Tera Debug] Now running " + Thread.currentThread().getStackTrace()[1].getMethodName() + " ...");
        try {
            TeraTableImpl table = new TeraTableImpl(tableName, "");

            byte[] rowKey = "reader1".getBytes();
            TeraMutationImpl mutation = table.newMutation(rowKey);
            assertTrue(mutation.add("cf21".getBytes(), "column1".getBytes(), "value1".getBytes()));
            assertTrue(mutation.add("cf21".getBytes(), "column1".getBytes(), "value2".getBytes()));
            assertTrue(mutation.add("cf21".getBytes(), "column2".getBytes(), "value3".getBytes()));
            assertTrue(mutation.add("cf22".getBytes(), "column3".getBytes(), "value4".getBytes()));
            table.applyMutation(mutation);
            table.flushCommits();

            TeraReaderImpl reader = table.newReader(rowKey);
            assertTrue(reader.add("cf21".getBytes(), null));
            assertTrue(reader.add("cf22".getBytes(), "column3".getBytes()));

            TeraResultImpl result = table.applyReader(reader);

            assertFalse(result.done());
            assertTrue(Arrays.equals(result.getFamily(), "cf21".getBytes()));
            assertTrue(Arrays.equals(result.getColumn(), "column1".getBytes()));
            assertTrue(Arrays.equals(result.getValue(), "value2".getBytes()));
            result.next();

            assertFalse(result.done());
            assertTrue(Arrays.equals(result.getFamily(), "cf21".getBytes()));
            assertTrue(Arrays.equals(result.getColumn(), "column2".getBytes()));
            assertTrue(Arrays.equals(result.getValue(), "value3".getBytes()));
            result.next();

            assertFalse(result.done());
            assertTrue(Arrays.equals(result.getFamily(), "cf22".getBytes()));
            assertTrue(Arrays.equals(result.getColumn(), "column3".getBytes()));
            assertTrue(Arrays.equals(result.getValue(), "value4".getBytes()));
            result.next();

            assertTrue(result.done());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

   @Test
    public void testUpdateInSingleMutation() {
        System.out.println("[Tera Debug] Now running " + Thread.currentThread().getStackTrace()[1].getMethodName() + " ...");
        try {
            TeraTableImpl table = new TeraTableImpl(tableName, "");

            byte[] rowKey = "update1".getBytes();
            TeraMutationImpl mutation = table.newMutation(rowKey);
            assertTrue(mutation.add("cf21".getBytes(), "column1".getBytes(), "value1".getBytes()));
            assertTrue(mutation.add("cf21".getBytes(), "column1".getBytes(), "value2".getBytes()));
            assertTrue(mutation.deleteColumn("cf21".getBytes(), "column1".getBytes()));
            table.applyMutation(mutation);

            TeraReaderImpl reader = table.newReader(rowKey);
            assertTrue(reader.add("cf21".getBytes(), null));
            TeraResultImpl result = table.applyReader(reader);
            assertFalse(result.done());
            assertTrue(result.equal(rowKey, "cf21".getBytes(), "column1".getBytes(), "value1".getBytes()));

            mutation = table.newMutation(rowKey);
            assertTrue(mutation.add("cf21".getBytes(), "column1".getBytes(), "value3".getBytes()));
            table.applyMutation(mutation);

            reader = table.newReader(rowKey);
            assertTrue(reader.add("cf21".getBytes(), null));
            result = table.applyReader(reader);
            assertFalse(result.done());
            assertTrue(result.equal(rowKey, "cf21".getBytes(), "column1".getBytes(), "value3".getBytes()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void clean() {
        try {
            assertTrue(client.disableTable(tableName));
            assertTrue(client.dropTable(tableName));
            System.out.println("[Tera Debug] Drop table:" + tableName);

        } catch (Exception e) {
            System.out.println("exception occured in TeraTableImplTest finalize: " + e.toString());
        }
    }
}
