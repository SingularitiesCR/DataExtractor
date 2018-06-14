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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Created by aleph on 3/19/18.
 * Singularities
 */
public final class SQLExtractor extends Extractor {

  protected final JdbcDialect dialect;
  protected ResultSet resultSet;
  protected boolean hasNext;
  protected int retries;

  protected final Properties properties;
  protected final String jdbcUrl;
  protected final String sqlQuery;
  protected final int fetchSize;

  protected SQLExtractor(ResultSet resultSet, int batchSize, JdbcDialect dialect, Properties properties,
                         String jdbcUrl, String sqlQuery, int fetchSize) {
    this.batchSize = batchSize;
    this.sparkSession = SparkSession.builder().getOrCreate();
    this.resultSet = resultSet;
    this.rowWidth = -1;
    this.hasNext = false;
    this.dialect = dialect;
    this.properties = properties;
    this.jdbcUrl = jdbcUrl;
    this.sqlQuery = sqlQuery;
    this.fetchSize = fetchSize;
    this.retries = 0;
  }

  @Override
  public Dataset<Row> nextBatch() throws SQLException {
    this.retries = 0;
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

    System.out.println(resultSet.getRow());

    // Just simulating an error fetching a line
    if ( resultSet.getRow() == 35 ){
      this.resetResultSet();
      this.retries++;
    }

    Object[] objects = new Object[this.rowWidth];
    for (int i = 0; i < this.rowWidth; i++) {
      DataType dataType = schema.fields()[i].dataType();
      if (dataType == DataTypes.IntegerType){
        objects[i] = resultSet.getInt(i+1);
      } else if (dataType == DataTypes.DoubleType){
        objects[i] = resultSet.getDouble(i+1);
      } else if (dataType == DataTypes.LongType){
        objects[i] = resultSet.getLong(i+1);
      } else if (dataType == DataTypes.FloatType){
        objects[i] = resultSet.getFloat(i+1);
      } else if (dataType == DataTypes.ShortType){
        objects[i] = resultSet.getShort(i+1);
      } else if (dataType == DataTypes.BinaryType){
        objects[i] = resultSet.getBytes(i+1); // todo check
      } else if (dataType == DataTypes.BooleanType){
        objects[i] = resultSet.getBoolean(i+1);
      } else if (dataType == DataTypes.ByteType){
        objects[i] = resultSet.getByte(i+1);
      } else if (dataType == DataTypes.DateType){
        objects[i] = resultSet.getDate(i+1);
      } else if (dataType == DataTypes.CalendarIntervalType){
        objects[i] = resultSet.getObject(i+1); //todo fix in case of use
      } else if (dataType == DataTypes.NullType){
        objects[i] = resultSet.getObject(i+1); // todo fix in case of use
      } else if (dataType == DataTypes.StringType){
        objects[i] = resultSet.getString(i+1); //todo check case of N-string
      } else if (dataType == DataTypes.TimestampType){
        objects[i] = resultSet.getTimestamp(i+1);
      } else {
//        System.err.println("Unable to detect type " + dataType.typeName() + " with known types");
        objects[i] = resultSet.getObject(i+1);
      }
    }
    hasNext = resultSet.next();
    return RowFactory.create(objects);
  }

  void resetResultSet() throws SQLException{
    System.out.println("Simulated error, recreating connection.");
    System.out.println("Retries: " + this.retries);
    int actualRow = this.resultSet.getRow();
    Connection conn = DriverManager.getConnection(this.jdbcUrl, this.properties);
    conn.setReadOnly(true);
    Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.setFetchSize(fetchSize);
    this.resultSet = statement.executeQuery(this.sqlQuery);

    while(this.resultSet.getRow() < actualRow){
      this.resultSet.next();
    }

  }

  public static final class SQLExtractorBuilder{
    private ResultSet resultSet;
    private int batchSize;
    private JdbcDialect dialect;
    private Properties properties;
    String jdbcUrl;
    String sqlQuery;
    int fetchSize;

    public SQLExtractor build(){
      return new SQLExtractor(resultSet, batchSize, dialect, properties, jdbcUrl, sqlQuery, fetchSize);
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

    public SQLExtractorBuilder setProperties(Properties properties){
      this.properties = properties;
      return this;
    }

    public SQLExtractorBuilder setJdbUrl(String jdbcUrl){
      this.jdbcUrl = jdbcUrl;
      return this;
    }

    public SQLExtractorBuilder setSqlQuery(String sqlQuery){
      this.sqlQuery = sqlQuery;
      return this;
    }

    public SQLExtractorBuilder setFetchSize(int fetchSize){
      this.fetchSize = fetchSize;
      return this;
    }

  }
}
