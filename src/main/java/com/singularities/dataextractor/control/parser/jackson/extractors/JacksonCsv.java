package com.singularities.dataextractor.control.parser.jackson.extractors;

import com.singularities.dataextractor.extractors.CsvExtractor;
import java.io.IOException;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

public class JacksonCsv {
  private String filename;
  private Integer batchSize;
  private String defaultStyle;
  private Character delimiter;
  private Character quoteChar;
  private String quoteMode;
  private Character commentStart;
  private Character escape;
  private Boolean ignoreSurroundingSpaces;
  private Boolean ignoreEmptyLines;
  private String recordSeparator;
  private String nullString;
  private List<Object> headerComments;
  private List<String> header;
  private Boolean skipHeaderRecord;
  private Boolean allowMissingColumnNames;
  private Boolean ignoreHeaderCase;
  private Boolean trim;
  private Boolean trailingDelimiter;

  public JacksonCsv() {
    filename = null;
    batchSize = null;
    defaultStyle = "DEFAULT";
    delimiter = null;
    quoteChar = null;
    quoteMode = null;
    commentStart = null;
    escape = null;
    ignoreSurroundingSpaces = null;
    ignoreEmptyLines = null;
    recordSeparator = null;
    nullString = null;
    headerComments = null;
    header = null;
    skipHeaderRecord = null;
    allowMissingColumnNames = null;
    ignoreHeaderCase = null;
    trim = null;
    trailingDelimiter = null;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(Integer batchSize) {
    this.batchSize = batchSize;
  }

  public String getDefaultStyle() {
    return defaultStyle;
  }

  public void setDefaultStyle(String defaultStyle) {
    this.defaultStyle = defaultStyle;
  }

  public Character getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(Character delimiter) {
    this.delimiter = delimiter;
  }

  public Character getQuoteChar() {
    return quoteChar;
  }

  public void setQuoteChar(Character quoteChar) {
    this.quoteChar = quoteChar;
  }

  public String getQuoteMode() {
    return quoteMode;
  }

  public void setQuoteMode(String quoteMode) {
    this.quoteMode = quoteMode;
  }

  public Character getCommentStart() {
    return commentStart;
  }

  public void setCommentStart(Character commentStart) {
    this.commentStart = commentStart;
  }

  public Character getEscape() {
    return escape;
  }

  public void setEscape(Character escape) {
    this.escape = escape;
  }

  public Boolean getIgnoreSurroundingSpaces() {
    return ignoreSurroundingSpaces;
  }

  public void setIgnoreSurroundingSpaces(Boolean ignoreSurroundingSpaces) {
    this.ignoreSurroundingSpaces = ignoreSurroundingSpaces;
  }

  public Boolean getIgnoreEmptyLines() {
    return ignoreEmptyLines;
  }

  public void setIgnoreEmptyLines(Boolean ignoreEmptyLines) {
    this.ignoreEmptyLines = ignoreEmptyLines;
  }

  public String getRecordSeparator() {
    return recordSeparator;
  }

  public void setRecordSeparator(String recordSeparator) {
    this.recordSeparator = recordSeparator;
  }

  public String getNullString() {
    return nullString;
  }

  public void setNullString(String nullString) {
    this.nullString = nullString;
  }

  public List<Object> getHeaderComments() {
    return headerComments;
  }

  public void setHeaderComments(List<Object> headerComments) {
    this.headerComments = headerComments;
  }

  public List<String> getHeader() {
    return header;
  }

  public void setHeader(List<String> header) {
    this.header = header;
  }

  public Boolean getSkipHeaderRecord() {
    return skipHeaderRecord;
  }

  public void setSkipHeaderRecord(Boolean skipHeaderRecord) {
    this.skipHeaderRecord = skipHeaderRecord;
  }

  public Boolean getAllowMissingColumnNames() {
    return allowMissingColumnNames;
  }

  public void setAllowMissingColumnNames(Boolean allowMissingColumnNames) {
    this.allowMissingColumnNames = allowMissingColumnNames;
  }

  public Boolean getIgnoreHeaderCase() {
    return ignoreHeaderCase;
  }

  public void setIgnoreHeaderCase(Boolean ignoreHeaderCase) {
    this.ignoreHeaderCase = ignoreHeaderCase;
  }

  public Boolean getTrim() {
    return trim;
  }

  public void setTrim(Boolean trim) {
    this.trim = trim;
  }

  public Boolean getTrailingDelimiter() {
    return trailingDelimiter;
  }

  public void setTrailingDelimiter(Boolean trailingDelimiter) {
    this.trailingDelimiter = trailingDelimiter;
  }

  private QuoteMode decodeQuote(){
    String upperCase = this.quoteMode.toUpperCase();
    switch (upperCase) {
      case "ALL":
        return  QuoteMode.ALL;
      case "ALL_NON_NULL":
        return QuoteMode.ALL_NON_NULL;
      case "MINIMAL":
        return QuoteMode.MINIMAL;
      case "NON_NUMERIC":
        return QuoteMode.NON_NUMERIC;
      case "NONE":
        return QuoteMode.NONE;
      default:
        throw new IllegalArgumentException("Quote Mode " + upperCase+" is not valid");
    }
  }

  private CSVFormat decodeDefaultFormat(){
    String upperCase = this.defaultStyle.toUpperCase();
    switch (upperCase) {
      case "DEFAULT":
        return CSVFormat.DEFAULT;
      case "EXCEL":
        return CSVFormat.EXCEL;
      case "INFORMIX_UNLOAD":
        return CSVFormat.INFORMIX_UNLOAD;
      case "INFORMIX_UNLOAD_CSV":
        return CSVFormat.INFORMIX_UNLOAD_CSV;
      case "MYSQL":
        return CSVFormat.MYSQL;
      case "POSTGRESQL_CSV":
        return CSVFormat.POSTGRESQL_CSV;
      case "POSTGRESQL_TEXT":
        return CSVFormat.POSTGRESQL_TEXT;
      case "RFC4180":
        return CSVFormat.RFC4180;
      case "TDF":
        return CSVFormat.TDF;
      default:
        throw new IllegalArgumentException("Defaukt Style " + upperCase+" is not valid");
    }
  }

  public CSVFormat decodeCSVFormat(){
    CSVFormat csvFormat = decodeDefaultFormat();
    if (delimiter != null){
      csvFormat = csvFormat.withDelimiter(this.delimiter);
    }
    if (quoteChar != null){
      csvFormat = csvFormat.withQuote(this.quoteChar);
    }
    if (quoteMode != null){
      csvFormat = csvFormat.withQuoteMode(decodeQuote());
    }
    if (commentStart != null){
      csvFormat = csvFormat.withCommentMarker(this.commentStart);
    }
    if (escape != null){
      csvFormat = csvFormat.withEscape(this.escape);
    }
    if (ignoreSurroundingSpaces != null){
      csvFormat = csvFormat.withIgnoreSurroundingSpaces(this.ignoreSurroundingSpaces);
    }
    if (ignoreEmptyLines != null){
      csvFormat = csvFormat.withIgnoreEmptyLines(this.ignoreEmptyLines);
    }
    if (recordSeparator != null){
      csvFormat = csvFormat.withRecordSeparator(this.recordSeparator);
    }
    if (nullString != null){
      csvFormat = csvFormat.withNullString(this.nullString);
    }
    if (headerComments != null){
      int size = this.headerComments.size();
      Object[] local_header = new String[size];
      for (int i = 0; i < size; i++) {
        local_header[i] = this.headerComments.get(i);
      }
      csvFormat = csvFormat.withHeaderComments(local_header);
    }
    if (header != null){
      int size = this.header.size();
      String[] local_header = new String[size];
      for (int i = 0; i < size; i++) {
        local_header[i] = this.header.get(i);
      }
      csvFormat = csvFormat.withHeader(local_header);
    }
    if (skipHeaderRecord != null){
      csvFormat = csvFormat.withSkipHeaderRecord(this.skipHeaderRecord);
    }
    if (allowMissingColumnNames != null){
      csvFormat = csvFormat.withAllowMissingColumnNames(this.allowMissingColumnNames);
    }
    if (ignoreHeaderCase != null){
      csvFormat = csvFormat.withIgnoreHeaderCase(this.ignoreHeaderCase);
    }
    if (trim != null){
      csvFormat = csvFormat.withTrim(this.trim);
    }
    if (trailingDelimiter != null){
      csvFormat = csvFormat.withTrailingDelimiter(this.trailingDelimiter);
    }
    return csvFormat;
  }

  public CsvExtractor createExtractor() throws IOException {
    CSVFormat csvFormat = decodeCSVFormat();
    return batchSize == null
        ? new CsvExtractor(filename, csvFormat)
        : new CsvExtractor(filename, csvFormat, batchSize);
  }
}