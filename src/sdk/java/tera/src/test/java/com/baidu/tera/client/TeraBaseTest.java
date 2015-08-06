package com.baidu.tera.client;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit test for TeraBase.
 */
public class TeraBaseTest
{
    public TeraBaseTest()
    {

    }
    @Test
    public void testBasicFeature()
    {
        System.out.println("Now running " + Thread.currentThread().getStackTrace()[1].getMethodName() + " ...");

        TeraBase teraBase = new TeraBase();
        // teraBase.InitGlog("tera-base-test");
        teraBase.LOG("INFO", "JAVA: LOG test");
        teraBase.LOG_INFO("JAVA: LOG_INFO test");
        teraBase.LOG_WARNING("JAVA: LOG_WARNING test");
        // teraBase.LOG_ERROR("JAVA: LOG_ERROR test");
        teraBase.VLOG(0, "JAVA: VLOG test level 0");
        // System.out.println(teraBase.getNativeMessage());
        assertTrue(true);
    }
}