package com.singularities.dataextractor.extractors;

import com.google.common.collect.Lists;
import com.monitorjbl.xlsx.StreamingReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public final class XlsxExtractor extends LineReaderExtractor {

  private Iterator<org.apache.poi.ss.usermodel.Row> iterator;
  private final Iterator<String> sheets;
  private final boolean allSheetsHaveHeader;
  private final Workbook workbook;

  private XlsxExtractor(String filename, Iterator<String> sheets, boolean hasHeader, boolean allSheetsHaveHeader,
                        int batchSize) throws FileNotFoundException {
    if (filename == null){
      throw new IllegalArgumentException("Filename is not set");
    }
    if (sheets == null){
      throw new IllegalArgumentException("Sheets is not set");
    }
    this.sparkSession =  SparkSession.builder().getOrCreate();
    this.batchSize = batchSize;
    this.hasHeader = hasHeader;
    this.rowWidth = -1;

    this.sheets = sheets;
    this.allSheetsHaveHeader  = allSheetsHaveHeader;
    InputStream stream = new FileInputStream(new File(filename));
    workbook = StreamingReader.builder().rowCacheSize(batchSize).open(stream);
    iterator = getNextRowIterator();
    // Read schema
    if (this.hasHeader) {
      this.schema = new StructType(Lists.newArrayList(iterator.next().iterator()).stream()
          .map(c -> new StructField(sanitizeSparkName(c.getStringCellValue()), DataTypes.StringType, true, Metadata.empty())).toArray(StructField[]::new));
      this.rowWidth = this.schema.length();
    }
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext() || sheets.hasNext();
  }

  @Override
  public Row readNext() {
    if (!iterator.hasNext()){
      iterator = getNextRowIterator();
      if (this.hasHeader && this.allSheetsHaveHeader){
        //discard the header
        iterator.next();
      }
    }
    org.apache.poi.ss.usermodel.Row localRow = iterator.next();
    if (rowWidth < 0){
      rowWidth = localRow.getLastCellNum();
    }
    Object[] acc = new Object[rowWidth];
    for (int i = 0; i < rowWidth; i++) {
      Cell cell = localRow.getCell(i);
      if (cell == null){
        acc[i] = null;
      } else {
        acc[i] = cell.getStringCellValue();
      }

    }
    return RowFactory.create(acc);
  }

  private Iterator<org.apache.poi.ss.usermodel.Row> getNextRowIterator() {
    return workbook.getSheet(this.sheets.next()).rowIterator();
  }

  public static class XlsxExtractorBuilder {
    private String filename = null;
    private Iterator<String> sheets = null;
    private boolean hasHeader = false;
    private boolean allSheetsHaveHeader = false;
    private int batchSize = Extractor.DEFAULT_BATCH;

    public XlsxExtractor build() throws FileNotFoundException {
      return new XlsxExtractor(filename, sheets, hasHeader, allSheetsHaveHeader, batchSize);
    }

    public XlsxExtractorBuilder setFilename(String filename) {
      this.filename = filename;
      return this;
    }

    public XlsxExtractorBuilder setSheets(Iterator<String> sheets) {
      this.sheets = sheets;
      return this;
    }

    public XlsxExtractorBuilder setSheets(Iterable<String> sheets) {
      this.sheets = sheets.iterator();
      return this;
    }

    public XlsxExtractorBuilder setSheet(String sheet){
      Collection<String> list = new ArrayList<>(1);
      list.add(sheet);
      this.sheets = list.iterator();
      return this;
    }

    public XlsxExtractorBuilder setHasHeader(boolean hasHeader) {
      this.hasHeader = hasHeader;
      return this;
    }

    public XlsxExtractorBuilder setAllSheetsHaveHeader(boolean allSheetsHaveHeader) {
      this.allSheetsHaveHeader = allSheetsHaveHeader;
      return this;
    }

    public XlsxExtractorBuilder setBatchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public String getFilename() {
      return filename;
    }

    public Iterator<String> getSheets() {
      return sheets;
    }

    public boolean isHasHeader() {
      return hasHeader;
    }

    public boolean isAllSheetsHaveHeader() {
      return allSheetsHaveHeader;
    }

    public int getBatchSize() {
      return batchSize;
    }
  }



}
