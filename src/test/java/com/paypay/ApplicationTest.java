package com.paypay;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

public class ApplicationTest {

    SparkSession sparkSession = SparkHelper.getSparkSession();



    @Test
    public void readFileTest(){

        Assert.assertEquals(Application.readFile("src/test/resources/data.csv", sparkSession).count(), 20);

    }


    @Test
    public void sessionizeDataTest(){

        Dataset<Row> df = Application.readFile("src/test/resources/data.csv", sparkSession);

        df = Application.sessionizeData("src/test/output/", df);

        df = df.where("client_ip = '123.242.248.130' and sessionId = '6e415e919f6bd574a73daf200791e5932d056c033e7e6c05c7ecf999de59ec5b'");

        Assert.assertTrue(df.count() == 4);

    }
}
