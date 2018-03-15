package com.singularities.dataextractor.extractors;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CsvExtractor extends LineReaderExtractor {

  private Iterator<CSVRecord> iterator;
  private CSVParser csvParser;

  public CsvExtractor(String filename, CSVFormat format) throws IOException {
    this(filename, format, Extractor.DEFAULT_BATCH);
  }

  public CsvExtractor(String filename, CSVFormat format, int batchSize) throws IOException {
    this.sparkSession = SparkSession.builder().getOrCreate();
    this.batchSize =  batchSize;
    this.csvParser = new CSVParser(new FileReader(filename), format);
    this.iterator = csvParser.iterator();
    this.size = -1;
  }


  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Row readNext() {
    CSVRecord next = iterator.next();
    if (size < 0){
      size = next.size();
      Map<String, Integer> headerMap = csvParser.getHeaderMap();
      if (headerMap != null) {
        hasHeader = true;
        ArrayList<Map.Entry<String, Integer>> entries = new ArrayList<>(headerMap
            .entrySet());
        entries.sort(Comparator.comparing(Map.Entry::getValue));
        schema = new StructType(entries.stream().map(Map.Entry::getKey)
            .map(c -> new StructField(c, DataTypes.StringType, false, Metadata.empty()))
            .toArray(StructField[]::new));
      } else {
        hasHeader = false;
        schema = getSchema();
      }
    }
    Object[] row = new Object[size];
    for (int i = 0; i < size; i++) {
      row[i] = next.get(i);
    }
    return RowFactory.create(row);
  }

}
