package com.singularities.dataextractor.extractors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class Extractor {

    protected int rowOffset;
    protected int batchSize = 128;

    /**
     * Gets the next batch of data from a dataset
     * @return A sub dataset of the original
     */
    public abstract Dataset<Row> nextBatch();


    public int getRowOffset() {
        return rowOffset;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
