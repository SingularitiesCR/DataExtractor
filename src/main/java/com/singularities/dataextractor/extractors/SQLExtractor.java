package com.singularities.dataextractor.extractors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;

/**
 * Created by aleph on 3/19/18.
 * Singularities
 */
public final class SQLExtractor extends Extractor {

  protected final JdbcDialect dialect;
  protected final ResultSet resultSet;
  protected boolean hasNext;

  protected SQLExtractor(ResultSet resultSet, int batchSize, JdbcDialect dialect) {
    this.batchSize = batchSize;
    this.sparkSession = SparkSession.builder().getOrCreate();
    this.resultSet = resultSet;
    this.rowWidth = -1;
    this.hasNext = false;
    this.dialect = dialect;
  }

  @Override
  public Dataset<Row> nextBatch() throws SQLException {
    List<Row> acc = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize && hasNext(); i++) {
      acc.add(readNext());
    }
    return sparkSession.createDataFrame(acc, this.schema);
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public boolean hasNext() throws SQLException {
    if (hasNext) {
      return true;
    }
    hasNext = resultSet.next();
    return hasNext;
  }

  @Override
  protected Row readNext() throws SQLException {
    if (rowWidth < 0){
      ResultSetMetaData metaData = resultSet.getMetaData();
      this.rowWidth =  metaData.getColumnCount();
      this.schema = JdbcUtils.getSchema(resultSet, dialect, true);
    }
    Object[] objects = new Object[this.rowWidth];
    for (int i = 0; i < this.rowWidth; i++) {
      objects[i] = resultSet.getObject(i+1);
    }
    hasNext = resultSet.next();
    return RowFactory.create(objects);
  }

  public static final class SQLExtractorBuilder{
    private ResultSet resultSet;
    private int batchSize;
    private JdbcDialect dialect;

    public SQLExtractor build(){
      return new SQLExtractor(resultSet, batchSize, dialect);
    }

    public SQLExtractorBuilder setResultSet(ResultSet resultSet) {
      this.resultSet = resultSet;
      return this;
    }

    public SQLExtractorBuilder setBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public SQLExtractorBuilder setDialect(JdbcDialect dialect) {
      this.dialect = dialect;
      return this;
    }

    public SQLExtractorBuilder setDialect(String url){
      this.dialect = JdbcDialects.get(url);
      return this;
    }

    public SQLExtractorBuilder setMySQLDialect() {
      this.dialect = JdbcDialects.get("jdbc:mysql");
      return this;
    }

    public SQLExtractorBuilder setBD2Dialect(){
      this.dialect = JdbcDialects.get("jdbc:db2");
      return this;
    }

    public SQLExtractorBuilder setMSSQLServerDialect(){
      this.dialect = JdbcDialects.get("jdbc:sqlserver");
      return this;
    }

    public SQLExtractorBuilder setDerbyDialect(){
      this.dialect = JdbcDialects.get("jdbc:derby");
      return this;
    }

    public SQLExtractorBuilder setOracleDialect(){
      this.dialect = JdbcDialects.get("jdbc:oracle");
      return this;
    }

    public SQLExtractorBuilder setOtherDialect(){
      this.dialect = JdbcDialects.get("");
      return this;
    }

    public SQLExtractorBuilder setPostgresDialect(){
      this.dialect = JdbcDialects.get("jdbc:postgresql");
      return this;
    }

    public SQLExtractorBuilder setResultSet(String url, Properties connectionProperties,
                                            String query, int fetchSize) throws SQLException {
      Connection conn = DriverManager.getConnection(url, connectionProperties);
      conn.setReadOnly(true);
      Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(fetchSize);
      resultSet = statement.executeQuery(query);
      batchSize = fetchSize;
      return this;
    }
  }
}
