package com.singularities.dataextractor.control.parser.jackson.extractors;

import com.google.common.collect.Maps;
import com.singularities.dataextractor.extractors.CsvExtractor;
import com.singularities.dataextractor.extractors.SQLExtractor;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class JacksonSqlTest {

  private static final String DATABASE_ENV_NAME = "DATABASE_ENV_NAME";
  private static final String DATABASE_ENV_HOST = "DATABASE_ENV_HOST";
  private static final String DATABASE_ENV_PORT = "DATABASE_ENV_PORT";
  private static final String DATABASE_ENV_USER = "DATABASE_ENV_USER";
  private static final String DATABASE_ENV_PASS = "DATABASE_ENV_PASS";
  private static final String DATABASE_ENV_TABLE = "DATABASE_ENV_TABLE";

  @Test
  void createExtractor() throws SQLException {
    SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("test sql")
        .getOrCreate();
    sparkSession.sparkContext().setLogLevel("ERROR");

    String database_name = System.getenv(DATABASE_ENV_NAME);
    String database_host = System.getenv(DATABASE_ENV_HOST);
    String database_port = System.getenv(DATABASE_ENV_PORT);
    String database_user = System.getenv(DATABASE_ENV_USER);
    String database_pass = System.getenv(DATABASE_ENV_PASS);
    String database_table = System.getenv(DATABASE_ENV_TABLE);

    assertNotNull(database_name);
    assertNotNull(database_host);
    assertNotNull(database_port);
    assertNotNull(database_user);
    assertNotNull(database_pass);
    assertNotNull(database_table);

    String url = String.format("jdbc:postgresql://%s:%s/%s", database_host, database_port, database_name);

    Properties connectionProperties = new Properties();
    connectionProperties.put("user", database_user);
    connectionProperties.put("password", database_pass);
    connectionProperties.put("socketTimeout", "15");

    HashMap<String, String> map = new HashMap<>(3);
    map.put("user", database_user);
    map.put("password", database_pass);
    map.put("socketTimeout", "15");
    String sql = String.format("select * from %s", database_table);
    JacksonSql jacksonSql = new JacksonSql();
    jacksonSql.setJdbcUrl(url);
    jacksonSql.setConectionProperties(map);
    jacksonSql.setFetchSize(10000);
    jacksonSql.setSqlQuery(sql);

    SQLExtractor extractor = jacksonSql.createExtractor();

    Dataset<Row> batch = extractor.nextBatch();
    int first = batch.first().getInt(0);
    assertEquals(1, first);

  }

  @Ignore
  @Test
  public void testControlFile() throws IOException, SQLException {
    // TODO
    SparkSession.builder().master("local[*]").appName("CSV Extractor").getOrCreate();
    String path = Objects.requireNonNull(getClass().getClassLoader().getResource("sqlControl.json")).getPath();
    ObjectMapper mapper = new ObjectMapper();
    JacksonSql control;
    control = mapper.readValue(new File(path), JacksonSql.class);
    SQLExtractor extractor = control.createExtractor();
    Dataset<Row> batch = extractor.nextBatch();
    String first = batch.first().getString(0);
    assertEquals("A", first);
  }
}