package com.singularities.dataextractor.extractors;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;
import org.apache.commons.csv.CSVFormat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvExtractorTest {

  private String filename;
  private CsvExtractor extractor;

  private String getFilename(String name) {
    return Objects.requireNonNull(getClass().getClassLoader().getResource(name)).getPath();
  }

  @BeforeEach
  void setUp() {
    SparkSession session = SparkSession.builder().master("local[*]").appName("XLS Test").getOrCreate();
    session.sparkContext().setLogLevel("ERROR");
  }

  @Test
  void load() throws IOException {
    filename = getFilename("data.csv");
    extractor = new CsvExtractor(filename, CSVFormat.DEFAULT);
    assertNotNull(extractor.getSchema());
  }

  @Test
  void loadNoHeaders() throws IOException, SQLException {
    filename = getFilename("noHeaders.csv");
    extractor = new CsvExtractor(filename, CSVFormat.DEFAULT.withSkipHeaderRecord());
    extractor.nextBatch();
    StructType schema = extractor.getSchema();
    assertNotNull(schema);

    assertEquals(5, schema.fieldNames().length);
    assertEquals("H0", schema.fieldNames()[0]);
  }

  @Test
  void nextBatch() throws IOException, SQLException {
    filename = getFilename("data.csv");
    extractor = new CsvExtractor(filename, CSVFormat.DEFAULT.withHeader(), 10);
    Dataset<Row> batch = extractor.nextBatch();
    assertNotNull(batch);
    assertTrue(extractor.batchSize <= batch.count());
    assertEquals("A", batch.first().get(0));
    assertEquals("B", extractor.nextBatch().first().get(0));
    assertEquals("C", extractor.nextBatch().first().get(0));
    assertEquals(0, extractor.nextBatch().count());
  }
}