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

//    postgres();
    sqlserverAzure();
  }

  private static void postgres() throws InterruptedException {
    System.out.println(ManagementFactory.getRuntimeMXBean().getName());

    Thread.sleep(20000);
    Properties connectionProperties = new Properties();
    connectionProperties.put("user", POSTGRES);
    connectionProperties.put("password", POSTGRES);

    SparkSession sparkSession = SparkSession.builder().master("local[1]").appName("ss").getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");
    Dataset<Row> dataset = sparkSession.read()
        .jdbc("jdbc:postgresql://localhost:5432/postgres",
            "test_massive", connectionProperties);

    dataset.show(250000);
  }

  private static void sqlserverAzure() throws InterruptedException {
    System.out.println(ManagementFactory.getRuntimeMXBean().getName());

    Thread.sleep(20000);
    Properties connectionProperties = new Properties();
//    connectionProperties.put("user", POSTGRES);
//    connectionProperties.put("password", POSTGRES);

    SparkSession sparkSession = SparkSession.builder().master("local[1]").appName("ss").getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");
    System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ");
    Dataset<Row> dataset = sparkSession.read()
        .jdbc("jdbc:sqlserver://error-check-sing.database.windows.net:1433;database=error_test;user=aleph@error-check-sing;password=A_leph010615;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;",
            "SalesLT.Customer", connectionProperties);

    dataset.show(250000);
  }
}
