package com.singularities.dataextractor.extractors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class Extractor {

    public static final int DEFAULT_BATCH = 128;
    protected int batchSize;
    protected SparkSession sparkSession;

    /**
     * Gets the next batch of data from a dataset
     * @return A sub dataset of the original
     */
    public abstract Dataset<Row> nextBatch();

    public int getBatchSize() {
        return batchSize;
    }

}
