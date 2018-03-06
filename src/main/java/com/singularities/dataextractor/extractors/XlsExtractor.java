package com.singularities.dataextractor.extractors;

import org.apache.spark.sql.*;
import scala.Tuple2;

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
     * @param sheet
     * @param header
     */
    public XlsExtractor(String filename, String sheet, boolean header) {
        load(filename, sheet, header);
    }

    /**
     * Constructor of Data extractor for XLS files
     * @param filename
     * @param sheet
     * @param header
     * @param batchSize
     */
    public XlsExtractor(String filename, String sheet, boolean header, int batchSize) {
        this.batchSize = batchSize;
        load(filename, sheet, header);
    }

    /**
     * Loads an XLS file to a Dataset
     * @param filename
     * @param sheet
     * @param header
     */
    public void load(String filename, String sheet, boolean header) {
        SparkSession session = SparkSession.getActiveSession().get();
        this.load(filename, sheet, header, session);
    }

    /**
     * Loads an XLS file to a Dataset
     * @param filename
     * @param sheet
     * @param header
     */
    public void load(String filename, String sheet, boolean header, SparkSession session) {
        dataset = session.read()
            .format("com.crealytics.spark.excel")
            .option("sheetName", sheet)
            .option("useHeader", header)
            .option("treatEmptyValuesAsNulls", DEFAULT_EMPTY_NULLS)
            .option("inferSchema", DEFAULT_INFER_SCHEMA)
            .option("maxRowsInMemory", batchSize)
            .load(filename);
    }

    @Override
    public Dataset<Row> nextBatch() {
        SQLContext context = SparkSession.getActiveSession().get().sqlContext();
        Dataset<Row> returnable = dataset.limit(batchSize);
        long size = new Long(batchSize);
        dataset = context.createDataFrame(dataset.javaRDD()
            .zipWithIndex()
            .filter(r -> r._2() >= size)
            .map(Tuple2::_1),
            dataset.schema()
        );
        rowOffset++;
        return returnable;
    }
}
