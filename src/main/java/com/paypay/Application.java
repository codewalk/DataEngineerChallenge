package com.paypay;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class Application {

    public static void main(String[] args) {


        String outputDataPath = System.getProperty("output_data_path");

        if (outputDataPath == null) {
            outputDataPath = "output/";
        }

        String inputFile = System.getProperty("input_data_path");

        if (inputFile == null) {
            inputFile = "data\\2015_07_22_mktplace_shop_web_log_sample.log.gz";
        }


        System.out.println("************************************************************************************");
        System.out.println("************************************************************************************");
        System.out.println("************************************************************************************\n\n");
        System.out.println("Data output path: " + outputDataPath);
        System.out.println("Input data file : " + inputFile);
        System.out.println("ENV : " + SparkHelper.ENV);
        System.out.println("\n\n************************************************************************************");
        System.out.println("************************************************************************************");
        System.out.println("************************************************************************************");


        SparkSession spark = SparkHelper.getSparkSession();


        Dataset<Row> df = readFile(inputFile, spark);


        /**
         * 1. SessionedData
         */
        df = sessionizeData(outputDataPath, df);


        /**
         * 2.1. Average session time in entire logs
         */
        Dataset<Row> avgSession = df.select(avg(col("diff_event_millisec")));
        avgSession.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", " ").option("header", "true").mode("overwrite").save(outputDataPath + "\\AverageSession");


        /**
         * 2.2. Average session time per user/ip
         */
        Dataset<Row> avgSessionPerUser = df.groupBy("client_ip").avg("diff_event_millisec");
        avgSessionPerUser.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", " ").option("header", "true").mode("overwrite").save(outputDataPath + "\\AverageSessionPerUser");


        /**
         * 3. Number of Unique urls per session
         */

        Dataset<Row> uniqueUrlsPerSession = df.groupBy("client_ip", "sessionId").agg(collect_set("api_url").as("api_urls"));
        uniqueUrlsPerSession.withColumn("noOfApi", size(col("api_urls"))).show();
        uniqueUrlsPerSession = uniqueUrlsPerSession.drop("api_urls");
        uniqueUrlsPerSession.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", " ").option("header", "true").mode("overwrite").save(outputDataPath + "\\UniqueUrlsPerSession");


        /**
         * 4. Sorted list of IP's with most engaged userss
         */
        Dataset<Row> mostEngaged = df.groupBy("client_ip").sum("diff_event_millisec").orderBy(col("sum(diff_event_millisec)").desc());

        mostEngaged.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", " ").option("header", "true").mode("overwrite").save(outputDataPath + "\\EngagedUsers");


    }


    /**
     * Below dataframe gives contains all the ip's and their sessionId => sha(clientIP, SessionCounter, client_id(useragent))
     *
     * @param outputDataPath
     * @param df
     * @return
     */
    public static Dataset<Row> sessionizeData(String outputDataPath, Dataset<Row> df) {
        WindowSpec sessionWindow = Window.partitionBy("client_ip").orderBy("event_millisec");
        df = df.withColumn("client_ip", split(col("client_address"), ":").getItem(0))
                .withColumn("client_port", split(col("client_address"), ":").getItem(1))
                .withColumn("event_millisec", callUDF("dateConversionUdf", col("event_dttm")))
                .withColumn("pre_event_millisec", lag(col("event_millisec"), 1).over(sessionWindow))
                .withColumn("curated_event_millisec", coalesce(col("pre_event_millisec"), col("event_millisec")))
                .withColumn("diff_event_millisec", col("event_millisec").minus(col("curated_event_millisec")))
                .withColumn("is_new_session", when(col("diff_event_millisec").gt(1800000), 1).otherwise(0))
                .withColumn("sessionId", sha2(concat(col("client_ip"), sum(col("is_new_session")).over(sessionWindow), col("client_id")), 256))
        ;


        df = df.select("client_ip", "sessionId", "diff_event_millisec", "event_dttm", "event_millisec", "api_url", "client_id").
                //where("client_ip = '1.186.37.2'").
                        orderBy("client_ip", "event_millisec").cache();

        df.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", " ").option("header", "true").mode("overwrite").save(outputDataPath + "\\SessionizeData");
        return df;
    }

    public static Dataset<Row> readFile(String inputFile, SparkSession spark) {
        DecimalType decimalType = DataTypes.createDecimalType(15, 10);

        StructType schema = new StructType()
                .add("event_dttm", "string")
                .add("shop_id", "string")
                .add("client_address", "string")
                .add("address2", "string")
                .add("amt1", decimalType)
                .add("amt2", decimalType)
                .add("amt3", decimalType)
                .add("num1", "long")
                .add("num2", "long")
                .add("num3", "long")
                .add("num4", "long")
                .add("api_url", "string")
                .add("client_id", "string")
                .add("protocol", "string")
                .add("cipher", "string");


        return spark.read()
                .option("delimiter", " ")
                .schema(schema)
                .csv(inputFile);
    }

}
