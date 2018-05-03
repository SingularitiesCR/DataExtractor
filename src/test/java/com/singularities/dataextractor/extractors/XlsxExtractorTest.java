package com.singularities.dataextractor.extractors;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XlsxExtractorTest {

  private static final String SHEET1 = "Sheet1";
  private String filename;
  private XlsxExtractor extractor;

  private String getFilename(String name) {
    return Objects.requireNonNull(getClass().getClassLoader().getResource(name)).getPath();
  }

  @BeforeEach
  void setUp() {
    SparkSession session = SparkSession.builder().master("local[*]").appName("XLSX Test").getOrCreate();
    session.sparkContext().setLogLevel("ERROR");
  }

  @Test
  void load() throws FileNotFoundException {
    filename = getFilename("data.xlsx");
    extractor = new XlsxExtractor.XlsxExtractorBuilder().setBatchSize(10).setFilename(filename)
        .setSheet(SHEET1).setHasHeader(true).build();
    assertNotNull(extractor.getSchema());
  }

  @Test
  void loadNoHeaders() throws FileNotFoundException, SQLException {
    filename = getFilename("noHeaders.xlsx");
    extractor = new XlsxExtractor.XlsxExtractorBuilder().setBatchSize(10).setFilename(filename)
        .setSheet(SHEET1).setHasHeader(false).build();
    extractor.nextBatch();
    StructType schema = extractor.getSchema();
    assertNotNull(schema);
    assertEquals(5, schema.fieldNames().length);
    assertEquals("H0", schema.fieldNames()[0]);
  }

  @Test
  void nextBatch() throws FileNotFoundException, SQLException {
    filename = getFilename("data.xlsx");
    extractor = new XlsxExtractor.XlsxExtractorBuilder().setBatchSize(10).setFilename(filename)
        .setSheet(SHEET1).setHasHeader(true).build();
    Dataset<Row> batch = extractor.nextBatch();
    assertNotNull(batch);
    assertTrue(extractor.batchSize <= batch.count());
    assertEquals("A", batch.first().get(0));
    assertEquals("B", extractor.nextBatch().first().get(0));
    assertEquals("C", extractor.nextBatch().first().get(0));
    assertEquals(0, extractor.nextBatch().count());
  }
}
