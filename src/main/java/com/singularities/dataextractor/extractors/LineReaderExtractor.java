package com.singularities.dataextractor.extractors;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public abstract class LineReaderExtractor extends Extractor {

    protected StructType schema;

    private StructType getSchema(int size) {
        if (schema != null) {
            return schema;
        }
        schema = new StructType(IntStream.rangeClosed(0, size - 1)
            .boxed()
            .map(i -> new StructField("H" + i, DataTypes.StringType, false, Metadata.empty()))
            .toArray(StructField[]::new));
        return  schema;
    }

    public abstract boolean hasNext();

    public abstract Row readNext();

    private Dataset<Row> readBatch() {
        List<Row> batch = new ArrayList<>();
        int size = 1;
        while (hasNext() && batch.size() < batchSize) {
            Row row = readNext();
            batch.add(row);
            size = row.size();
        }
        SQLContext context = SparkSession.getActiveSession().get().sqlContext();
        return context.createDataFrame(
                batch, getSchema(size)
        );
    }

    @Override
    public Dataset<Row> nextBatch() {
        Dataset<Row> returnable = readBatch();
        rowOffset++;
        return returnable;
    }

    public StructType getSchema() {
        return schema;
    }
}
