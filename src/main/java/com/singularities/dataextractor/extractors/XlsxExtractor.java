package com.singularities.dataextractor.extractors;

import com.google.common.collect.Lists;
import com.monitorjbl.xlsx.StreamingReader;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class XlsxExtractor extends LineReaderExtractor {

    private Iterator<org.apache.poi.ss.usermodel.Row> iterator;

    /**
     * Constructor without loading dataset
     */
    public XlsxExtractor() {}

    /**
     * Constructor of Data extractor for XLS files
     * @param filename
     * @param sheet
     * @param header
     */
    public XlsxExtractor(String filename, String sheet, boolean header) throws FileNotFoundException {
        load(filename, sheet, header);
    }

    /**
     * Constructor of Data extractor for XLS files
     * @param filename
     * @param sheet
     * @param header
     * @param batchSize
     */
    public XlsxExtractor(String filename, String sheet, boolean header, int batchSize) throws FileNotFoundException {
        this.batchSize = batchSize;
        load(filename, sheet, header);
    }

    /**
     * Loads an XLS file to a Dataset
     * @param filename
     * @param sheet
     * @param header
     */
    public void load(String filename, String sheet, boolean header) throws FileNotFoundException {
        InputStream stream = new FileInputStream(new File(filename));
        Workbook workbook = StreamingReader.builder()
                .rowCacheSize(batchSize)
                .open(stream);
        this.iterator = workbook.getSheet(sheet).rowIterator();
        // Read schema
        if (header) {
            List<StructField> fields = Lists.newArrayList(this.iterator.next().iterator()).stream()
                .map(c -> new StructField(c.getStringCellValue(), DataTypes.StringType, false, Metadata.empty()))
                .collect(Collectors.toList());
            schema = new StructType(fields.toArray(new StructField[fields.size()]));
        }

    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Row readNext() {
        List<String> cells = new ArrayList<>();
        iterator.next().iterator().forEachRemaining(c -> cells.add(c.getStringCellValue()));
        return RowFactory.create(cells.toArray());
    }

}
