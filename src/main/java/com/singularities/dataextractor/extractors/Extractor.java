package com.singularities.dataextractor.extractors;

import java.sql.SQLException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public abstract class Extractor {

    public static final int DEFAULT_BATCH = 128;
    public static final String invalidPattern = "[ ,;{}()\\.\\n\\t=]";
    public static final String replace = "__";

    protected SparkSession sparkSession;
    protected int batchSize;
    protected int rowWidth;
    protected StructType schema;

    /**
     * Gets the next batch of data from a dataset
     * @return A sub dataset of the original
     */
    public abstract Dataset<Row> nextBatch() throws SQLException;

    public int getBatchSize() {
        return batchSize;
    }

    public abstract StructType getSchema();

    public abstract boolean hasNext() throws SQLException;

    protected abstract Row readNext() throws SQLException;

    public static String sanitizeSparkName(String name, String replaceWith){
        return name.replaceAll(invalidPattern, replaceWith);
    }
    public static String sanitizeSparkName(String name){
        return sanitizeSparkName(name, replace);
    }

}
