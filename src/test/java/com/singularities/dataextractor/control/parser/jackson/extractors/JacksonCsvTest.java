package com.singularities.dataextractor.control.parser.jackson.extractors;

import com.singularities.dataextractor.extractors.CsvExtractor;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by aleph on 3/22/18.
 * Singularities
 */
class JacksonCsvTest {


  @Test
  public void testDefault(){
    JacksonCsv jacksonCsv = new JacksonCsv();
    assertEquals(CSVFormat.DEFAULT, jacksonCsv.decodeCSVFormat());
  }

  @Test
  public void testFull(){
    JacksonCsv jacksonCsv = new JacksonCsv();
    jacksonCsv.setAllowMissingColumnNames(true);
    jacksonCsv.setBatchSize(10);
    jacksonCsv.setCommentStart('@');
    jacksonCsv.setDefaultStyle("EXCEL");
    jacksonCsv.setDelimiter(';');
    jacksonCsv.setEscape('9');
    jacksonCsv.setHeader(new ArrayList<>());
    jacksonCsv.setHeaderComments(new ArrayList<>());
    jacksonCsv.setIgnoreEmptyLines(true);
    jacksonCsv.setIgnoreHeaderCase(true);
    jacksonCsv.setIgnoreSurroundingSpaces(true);
    jacksonCsv.setNullString("~~~~");
    jacksonCsv.setQuoteChar('w');
    jacksonCsv.setQuoteMode("MINIMAL");
    jacksonCsv.setRecordSeparator("w");
    jacksonCsv.setSkipHeaderRecord(true);
    jacksonCsv.setTrailingDelimiter(true);
    jacksonCsv.setTrim(true);

    CSVFormat csvFormat = CSVFormat.EXCEL.withAllowMissingColumnNames(true)
        .withCommentMarker('@')
        .withDelimiter(';')
        .withEscape('9')
        .withHeader(new String[]{})
        .withHeaderComments(new Object[]{})
        .withIgnoreEmptyLines(true)
        .withIgnoreHeaderCase(true)
        .withIgnoreSurroundingSpaces(true)
        .withNullString("~~~~")
        .withQuote('w')
        .withQuoteMode(QuoteMode.MINIMAL)
        .withRecordSeparator("w")
        .withSkipHeaderRecord(true)
        .withTrailingDelimiter(true)
        .withTrailingDelimiter(true);

    assertEquals(csvFormat, jacksonCsv.decodeCSVFormat());
  }

  @Test
  public void testOpen() throws IOException, SQLException {
    SparkSession.builder().master("local[*]").appName("CSV Extractor").getOrCreate();
    String path = Objects.requireNonNull(getClass().getClassLoader().getResource("data.csv"))
        .getPath();
    JacksonCsv jacksonCsv = new JacksonCsv();
    jacksonCsv.setFilename(path);
    jacksonCsv.setBatchSize(250);
    jacksonCsv.setHeader(new ArrayList<>());
    CsvExtractor extractor = jacksonCsv.createExtractor();
    Dataset<Row> batch = extractor.nextBatch();
    String first = batch.first().getString(0);
    assertEquals("A", first);
  }

  @Test
  public void testControlFile() throws IOException, SQLException {
    SparkSession.builder().master("local[*]").appName("CSV Extractor").getOrCreate();
    String path = Objects.requireNonNull(getClass().getClassLoader().getResource("csvControl.json")).getPath();
    ObjectMapper mapper = new ObjectMapper();
    JacksonCsv control;
    control = mapper.readValue(new File(path), JacksonCsv.class);
    CsvExtractor extractor = control.createExtractor();
    Dataset<Row> batch = extractor.nextBatch();
    String first = batch.first().getString(0);
    assertEquals("A", first);
  }

}