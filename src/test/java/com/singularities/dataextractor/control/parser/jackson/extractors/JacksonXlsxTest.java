package com.singularities.dataextractor.control.parser.jackson.extractors;

import com.google.common.collect.Lists;
import com.singularities.dataextractor.extractors.SQLExtractor;
import com.singularities.dataextractor.extractors.XlsxExtractor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by aleph on 3/22/18.
 * Singularities
 */
class JacksonXlsxTest {

  @Test
  void createExtractor() throws FileNotFoundException, SQLException {
    SparkSession sparkSession = SparkSession.builder().master("local[*]").appName
        ("JacksonXlsxTest").getOrCreate();

    String path = Objects.requireNonNull(getClass().getClassLoader().getResource("data.xlsx"))
        .getPath();

    JacksonXlsx jacksonXlsx = new JacksonXlsx();
    jacksonXlsx.setFilename(path);
    jacksonXlsx.setBatchSize(200);
    jacksonXlsx.setHasHeader(true);
    jacksonXlsx.setSheets(Lists.newArrayList("Sheet1"));
    XlsxExtractor extractor = jacksonXlsx.createExtractor();
    Dataset<Row> batch = extractor.nextBatch();
    String first = batch.first().getString(0);
    assertEquals("A", first);
  }

  @Test
  public void testControlFile() throws IOException, SQLException {
    SparkSession.builder().master("local[*]").appName("CSV Extractor").getOrCreate();
    String path = Objects.requireNonNull(getClass().getClassLoader().getResource("xlsxControl.json")).getPath();
    ObjectMapper mapper = new ObjectMapper();
    JacksonXlsx control;
    control = mapper.readValue(new File(path), JacksonXlsx.class);
    XlsxExtractor extractor = control.createExtractor();
    Dataset<Row> batch = extractor.nextBatch();
    String first = batch.first().getString(0);
    assertEquals("A", first);
  }
}