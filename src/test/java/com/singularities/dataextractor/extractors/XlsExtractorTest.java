package com.singularities.dataextractor.extractors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class XlsExtractorTest {

    private String filename;
    private XlsExtractor extractor;
    private SparkSession session;

    @BeforeEach
    void setUp() {
        filename = getClass().getClassLoader().getResource("data.xls").toString();
        session = SparkSession.builder().master("local[*]").appName("XLS Test").getOrCreate();
        extractor = new XlsExtractor();
        extractor.setBatchSize(10);
    }

    @Test
    void load() {
        extractor.load(filename, "Sheet1", true, session);
        assertNotNull(extractor.dataset);
    }

    @Test
    void nextBatch() {
        extractor.load(filename, "Sheet1", true, session);
        Dataset<Row> batch = extractor.nextBatch();
        assertNotNull(batch);
        assertTrue(extractor.batchSize <= batch.count());
        assertEquals("A", batch.first().get(0));
        assertEquals("B", extractor.nextBatch().first().get(0));
        assertEquals("C", extractor.nextBatch().first().get(0));
        assertEquals(0, extractor.dataset.count());
    }
}