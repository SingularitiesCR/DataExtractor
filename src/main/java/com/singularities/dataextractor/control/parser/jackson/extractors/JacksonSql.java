package com.singularities.dataextractor.control.parser.jackson.extractors;

import com.singularities.dataextractor.control.parser.model.ParserExtractor;
import com.singularities.dataextractor.extractors.SQLExtractor;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class JacksonSql implements ParserExtractor {
  private static final int defaultFetchSize = 10000;
  private String jdbcUrl;
  private Map<String, String> conectionProperties;
  private String sqlQuery;
  private String sqlDialect;
  private Integer fetchSize;

  public JacksonSql() {
    jdbcUrl = null;
    conectionProperties = null;
    sqlQuery = null;
    sqlDialect = null;
    fetchSize = null;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public Map<String, String> getConectionProperties() {
    return conectionProperties;
  }

  public void setConectionProperties(Map<String, String> conectionProperties) {
    this.conectionProperties = conectionProperties;
  }

  public String getSqlQuery() {
    return sqlQuery;
  }

  public void setSqlQuery(String sqlQuery) {
    this.sqlQuery = sqlQuery;
  }

  public String getSqlDialect() {
    return sqlDialect;
  }

  public void setSqlDialect(String sqlDialect) {
    this.sqlDialect = sqlDialect;
  }

  public Integer getFetchSize() {
    return fetchSize;
  }

  public void setFetchSize(Integer fetchSize) {
    this.fetchSize = fetchSize;
  }

  private Properties decodeConectionProperties(){
    Properties properties = new Properties();
    properties.putAll(conectionProperties);
    return properties;
  }

  @SuppressWarnings("HardCodedStringLiteral")
  private SQLExtractor.SQLExtractorBuilder decodeDialect(SQLExtractor.SQLExtractorBuilder acc){
    if (sqlDialect == null) {
      return acc.setDialect(jdbcUrl);
    }
    String lowerCase = sqlDialect.toLowerCase();
    switch (lowerCase) {
      case "mysql":
        return acc.setMySQLDialect();
      case "db2":
        return acc.setBD2Dialect();
      case "sqlserver":
        return acc.setMSSQLServerDialect();
      case "derby":
        return acc.setDerbyDialect();
      case "postgresql":
        return acc.setPostgresDialect();
      case "oracle":
        return acc.setOracleDialect();
      case "other":
        return acc.setOtherDialect();
      default:
        throw new IllegalArgumentException("SQL dialect " + lowerCase + "is no valid");
    }

  }

  @Override
  public SQLExtractor createExtractor() throws SQLException {
    SQLExtractor.SQLExtractorBuilder sqlExtractorBuilder = new SQLExtractor.SQLExtractorBuilder();
    if (jdbcUrl == null){
      throw new IllegalArgumentException("URL is required");
    }
    if (conectionProperties == null){
      throw new IllegalArgumentException("Connection Properties are required");
    }
    if (sqlQuery == null){
      throw new IllegalArgumentException("SQL query is required");
    }
    int bz = this.fetchSize == null ? defaultFetchSize : this.fetchSize;

    sqlExtractorBuilder.setProperties(decodeConectionProperties());
    sqlExtractorBuilder.setJdbUrl(jdbcUrl);
    sqlExtractorBuilder.setSqlQuery(sqlQuery);
    sqlExtractorBuilder.setFetchSize(bz);
    sqlExtractorBuilder.setResultSet(jdbcUrl, decodeConectionProperties(), sqlQuery, bz);
    return decodeDialect(sqlExtractorBuilder).build();
  }

}
