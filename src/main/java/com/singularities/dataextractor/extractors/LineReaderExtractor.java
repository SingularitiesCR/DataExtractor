package com.singularities.dataextractor.extractors;

import java.sql.SQLException;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public abstract class LineReaderExtractor extends Extractor {

  protected boolean hasHeader;


  @Override
  public StructType getSchema() {
    if (schema == null) {
      schema = new StructType(IntStream.rangeClosed(0, rowWidth - 1)
          .boxed()
          .map(i -> new StructField("H" + i, DataTypes.StringType, false, Metadata.empty()))
          .toArray(StructField[]::new));

    }
    return schema;

  }

  private Dataset<Row> readBatch() throws SQLException {
    List<Row> batch = new ArrayList<>();
    while (hasNext() && batch.size() < batchSize) {
      Row row = readNext();
      batch.add(row);
    }

    return sparkSession.createDataFrame(batch, getSchema());
  }

  @Override
  public Dataset<Row> nextBatch() throws SQLException {
    return readBatch();
  }
}
