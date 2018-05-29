package com.singularities.dataextractor.extractors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Created by aleph on 3/19/18.
 * Singularities
 */
class SQLExtractorTest {

  private static final String DATABASE_ENV_NAME = "DATABASE_ENV_NAME";
  private static final String DATABASE_ENV_HOST = "DATABASE_ENV_HOST";
  private static final String DATABASE_ENV_PORT = "DATABASE_ENV_PORT";
  private static final String DATABASE_ENV_USER = "DATABASE_ENV_USER";
  private static final String DATABASE_ENV_PASS = "DATABASE_ENV_PASS";
  private static final String DATABASE_ENV_TABLE = "DATABASE_ENV_TABLE";



  private static SparkSession sparkSession;
  private static Properties connectionProperties;

  private static String database_table;
  private static String url;


  @BeforeAll
  public static void setup(){
    sparkSession = SparkSession.builder().master("local[*]").appName("test sql")
        .getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    String database_name = System.getenv(DATABASE_ENV_NAME);
    String database_host = System.getenv(DATABASE_ENV_HOST);
    String database_port = System.getenv(DATABASE_ENV_PORT);
    String database_user = System.getenv(DATABASE_ENV_USER);
    String database_pass = System.getenv(DATABASE_ENV_PASS);
    database_table = System.getenv(DATABASE_ENV_TABLE);

    Assertions.assertNotNull(database_name);
    Assertions.assertNotNull(database_host);
    Assertions.assertNotNull(database_port);
    Assertions.assertNotNull(database_user);
    Assertions.assertNotNull(database_pass);
    Assertions.assertNotNull(database_table);

    url = String.format("jdbc:postgresql://%s:%s/%s", database_host, database_port, database_name);

    connectionProperties = new Properties();
    connectionProperties.put("user", database_user);
    connectionProperties.put("password", database_pass);
    connectionProperties.put("socketTimeout", "15");
  }

  @Test
  public void testSQL() throws SQLException {
    Connection conn = DriverManager.getConnection(url, connectionProperties);
    String sql = String.format("select * from %s", database_table);
    SQLExtractor sqlExtractor = new SQLExtractor.SQLExtractorBuilder().setPostgresDialect()
        .setResultSet(url, connectionProperties, sql, 10000).build();

    List<Dataset<Row>> acc = new ArrayList<>();
    while (sqlExtractor.hasNext()){
      acc.add(sqlExtractor.nextBatch());
    }
    Dataset<Row> dataset = acc.parallelStream().reduce(Dataset::union).get();
    long count = dataset.count();
    Assertions.assertEquals(245057, count, "Data must have have rows");
    Assertions.assertEquals(5, dataset.schema().length(), "Data has only 5 cols");

  }

  //@Test
  public void vsSpark(){
    Dataset<Row> dataset = sparkSession.read()
        .jdbc(url,database_table, "id", 20, 800,
            20, connectionProperties);
    System.out.println(dataset.count());
//    dataset.write().parquet("sb_"+ UUID.randomUUID());
  }

}