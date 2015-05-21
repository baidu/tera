package com.baidu.tera.client;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Date;

/**
 * Unit test for TeraClientImpl.
 */
public class TeraClientImplTest
{
    public TeraClientImplTest()
    {

    }
    @Test
    public void testTeraClient()
    {
        System.out.println("Now running " + Thread.currentThread().getStackTrace()[1].getMethodName() + " ...");
        try {
            String tableName = "jni_client_test_" + Long.toString(System.currentTimeMillis() % 1000);
            TeraClientImpl teraClient = new TeraClientImpl();
            String schema = tableName + "{family1,family2,family3}";
            assertTrue(teraClient.createTable(tableName, schema));
            assertTrue(teraClient.disableTable(tableName));
            assertTrue(teraClient.enableTable(tableName));
            assertTrue(teraClient.getTableDescriptor(tableName).equals("family1,family2,family3"));
            assertNull(teraClient.getTableDescriptor("illegal"));
            assertTrue(teraClient.disableTable(tableName));
            assertTrue(teraClient.dropTable(tableName));
        } catch (Exception e) {
            System.out.println("exception occured in tera client test: " + e.toString());
        }
    }
}
