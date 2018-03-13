package com.singularities.dataextractor.extractors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class CsvExtractorTest {

    private String filename;
    private CsvExtractor extractor;

    private String getFilename(String name) {
        return Objects.requireNonNull(getClass().getClassLoader().getResource(name)).getPath();
    }

    @BeforeEach
    void setUp() {
        SparkSession session = SparkSession.builder().master("local[*]").appName("XLS Test").getOrCreate();
        SparkSession.setActiveSession(session);
        session.sparkContext().setLogLevel("ERROR");
        extractor = new CsvExtractor();
        extractor.setBatchSize(10);
    }

    @Test
    void load() throws FileNotFoundException {
        filename = getFilename("data.csv");
        extractor.load(filename, true);
        assertNotNull(extractor.getSchema());
    }

    @Test
    void loadNoHeaders() throws FileNotFoundException {
        filename = getFilename("noHeaders.csv");
        extractor.load(filename, false);
        extractor.nextBatch();
        StructType schema = extractor.getSchema();
        assertNotNull(schema);
        assertEquals(5, schema.fieldNames().length);
        assertEquals("H0", schema.fieldNames()[0]);
    }

    @Test
    void nextBatch() throws FileNotFoundException {
        filename = getFilename("data.csv");
        extractor.load(filename, true);
        Dataset<Row> batch = extractor.nextBatch();
        assertNotNull(batch);
        assertTrue(extractor.batchSize <= batch.count());
        assertEquals("A", batch.first().get(0));
        assertEquals("B", extractor.nextBatch().first().get(0));
        assertEquals("C", extractor.nextBatch().first().get(0));
        assertEquals(0, extractor.nextBatch().count());
    }


    @Test
    void memoryTest() throws FileNotFoundException {
        filename = getFilename("bigCsv.csv");
        extractor.setBatchSize(1000);
        extractor.load(filename, true);
        Dataset<Row> batch = extractor.nextBatch();
        long count = batch.count();
        while (count > 0) {
            System.out.println(count);
            batch = extractor.nextBatch();
            count = batch.count();
        }
    }

}