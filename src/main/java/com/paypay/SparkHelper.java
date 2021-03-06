package com.paypay;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.time.Instant;

import static org.apache.spark.sql.functions.udf;

public class SparkHelper {

    private static SparkSession sparkSession;
    public static String ENV = System.getProperty("ENV") != null ? System.getProperty("ENV") : "LOCAL";

    public static SparkSession getSparkSession() {
        if (sparkSession == null) {

            if (ENV.equalsIgnoreCase("LOCAL")) {
                sparkSession = SparkSession.builder()
                        .appName("SimpleApp").config("spark.master", "local[*]").getOrCreate();
            } else {

                sparkSession = SparkSession.builder()
                        .appName("SimpleApp").getOrCreate();

            }
            registerUdf();

        }

        return sparkSession;
    }

    private static void registerUdf() {
        UserDefinedFunction dateConversionUdf = udf(
                (String strDate) -> Instant.parse(strDate).toEpochMilli(), DataTypes.LongType);

        sparkSession.sqlContext().udf().register("dateConversionUdf", dateConversionUdf);
    }

}


