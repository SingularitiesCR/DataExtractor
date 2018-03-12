package com.singularities.dataextractor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.lang.management.ManagementFactory;
import java.util.Properties;

public class DataExtractor {

  private static final String POSTGRES = "postgres";

  public static void main(String[] args) throws InterruptedException {
    // TODO

    System.out.println(ManagementFactory.getRuntimeMXBean().getName());

    Thread.sleep(10000);
    Properties connectionProperties = new Properties();
    connectionProperties.put("user", POSTGRES);
    connectionProperties.put("password", POSTGRES);
    connectionProperties.put("socketTimeout", "15");


    SparkSession sparkSession = SparkSession.builder().master("local[1]").appName("ss").getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    Dataset<Row> dataset = sparkSession.read()
        .jdbc("jdbc:postgresql://192.168.1.50:32772/postgres",
            "test_massive", connectionProperties);

    dataset.show(250000);
  }
}
