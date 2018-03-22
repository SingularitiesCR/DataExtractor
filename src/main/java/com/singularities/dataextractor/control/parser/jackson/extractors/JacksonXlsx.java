package com.singularities.dataextractor.control.parser.jackson.extractors;

import com.singularities.dataextractor.extractors.XlsxExtractor;
import java.io.FileNotFoundException;
import java.util.List;

/**
 * Created by aleph on 3/22/18.
 * Singularities
 */
public class JacksonXlsx {
  private String filename;
  private List<String> sheets;
  private Boolean hasHeader;
  private Boolean allSheetsHaveHeader;
  private Integer batchSize;
  private String noColPrefix;
  private Integer numberSkipCols;
  private Integer numberSkipRows;

  public JacksonXlsx() {
    filename = null;
    sheets = null;
    hasHeader = null;
    allSheetsHaveHeader = null;
    batchSize = null;
    noColPrefix = null;
    numberSkipCols = null;
    numberSkipRows = null;
  }

  public String getFilename() {
    return filename;
  }

  public void setFilename(String filename) {
    this.filename = filename;
  }

  public List<String> getSheets() {
    return sheets;
  }

  public void setSheets(List<String> sheets) {
    this.sheets = sheets;
  }

  public Boolean getHasHeader() {
    return hasHeader;
  }

  public void setHasHeader(Boolean hasHeader) {
    this.hasHeader = hasHeader;
  }

  public Boolean getAllSheetsHaveHeader() {
    return allSheetsHaveHeader;
  }

  public void setAllSheetsHaveHeader(Boolean allSheetsHaveHeader) {
    this.allSheetsHaveHeader = allSheetsHaveHeader;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(Integer batchSize) {
    this.batchSize = batchSize;
  }

  public String getNoColPrefix() {
    return noColPrefix;
  }

  public void setNoColPrefix(String noColPrefix) {
    this.noColPrefix = noColPrefix;
  }

  public Integer getNumberSkipCols() {
    return numberSkipCols;
  }

  public void setNumberSkipCols(Integer numberSkipCols) {
    this.numberSkipCols = numberSkipCols;
  }

  public Integer getNumberSkipRows() {
    return numberSkipRows;
  }

  public void setNumberSkipRows(Integer numberSkipRows) {
    this.numberSkipRows = numberSkipRows;
  }

  public XlsxExtractor createExtractor() throws FileNotFoundException {
    XlsxExtractor.XlsxExtractorBuilder builder = new XlsxExtractor
        .XlsxExtractorBuilder();

    if (filename == null) {
      throw new IllegalArgumentException("Path is required");
    }
    builder.setFilename(filename);
    if (sheets == null) {
      throw new IllegalArgumentException("At least 1 sheet is required");
    }
    builder.setSheets(sheets);
    if (hasHeader != null) {
      builder.setHasHeader(hasHeader);
    }
    if (allSheetsHaveHeader != null) {
      builder.setAllSheetsHaveHeader(allSheetsHaveHeader);
    }
    if (batchSize != null) {
      builder.setBatchSize(batchSize);
    }
    if (noColPrefix != null) {
      builder.setNoColPrefix(noColPrefix);
    }
    if (numberSkipCols != null) {
      builder.setNumberSkipCols(numberSkipCols);
    }
    if (numberSkipRows != null) {
      builder.setNumberSkipRows(numberSkipRows);
    }
    return builder.build();
  }


}
