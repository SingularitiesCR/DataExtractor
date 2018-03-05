package com.singularities.dataextractor.extractors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class XlsExtractor extends Extractor {

    protected final boolean DEFAULT_EMPTY_NULLS = true;
    protected final boolean DEFAULT_INFER_SCHEMA = true;
    Dataset<Row> dataset;

    /**
     * Constructor without loading dataset
     */
    public XlsExtractor() {}

    /**
     * Constructor of Data extractor for XLS files
     * @param filename
     * @param Sheet
     * @param header
     */
    public XlsExtractor(String filename, String Sheet, boolean header) {
        load(filename, Sheet, header);
    }

    /**
     * Constructor of Data extractor for XLS files
     * @param filename
     * @param Sheet
     * @param header
     * @param batchSize
     */
    public XlsExtractor(String filename, String Sheet, boolean header, int batchSize) {
        this.batchSize = batchSize;
        load(filename, Sheet, header);
    }

    /**
     * Loads an XLS file to a Dataset
     * @param filename
     * @param Sheet
     * @param header
     */
    public void load(String filename, String Sheet, boolean header) {
        dataset = SparkSession.getActiveSession().get().read()
            .format("com.crealytics.spark.excel")
            .option("sheetName", Sheet)
            .option("useHeader", header)
            .option("treatEmptyValuesAsNulls", DEFAULT_EMPTY_NULLS)
            .option("inferSchema", DEFAULT_INFER_SCHEMA)
            .option("maxRowsInMemory", batchSize)
            .load(filename);
    }

    @Override
    public Dataset<Row> nextBatch() {
        SparkSession session = SparkSession.getActiveSession().get();
        Dataset<Row> returnable = session
                .createDataFrame(Arrays.asList(dataset.take(batchSize)), dataset.schema());
        // TODO
        rowOffset++;
        return returnable;
    }
}
