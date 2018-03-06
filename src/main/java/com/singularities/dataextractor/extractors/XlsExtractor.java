package com.singularities.dataextractor.extractors;

import com.google.common.collect.Lists;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import com.monitorjbl.xlsx.StreamingReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class XlsExtractor extends Extractor {

    private Sheet sheet;
    private StructType schema;

    /**
     * Constructor without loading dataset
     */
    public XlsExtractor() {}

    /**
     * Constructor of Data extractor for XLS files
     * @param filename
     * @param sheet
     * @param header
     */
    public XlsExtractor(String filename, String sheet, boolean header) throws FileNotFoundException {
        load(filename, sheet, header);
    }

    /**
     * Constructor of Data extractor for XLS files
     * @param filename
     * @param sheet
     * @param header
     * @param batchSize
     */
    public XlsExtractor(String filename, String sheet, boolean header, int batchSize) throws FileNotFoundException {
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
        this.sheet = workbook.getSheet(sheet);
        // Read schema
        if (header) {
            List<StructField> fields = Lists.newArrayList(this.sheet.rowIterator().next().iterator()).stream()
                .map(c -> new StructField(c.getStringCellValue(), DataTypes.StringType, false, Metadata.empty()))
                .collect(Collectors.toList());
            schema = new StructType(fields.toArray(new StructField[fields.size()]));
        }

    }

    private Dataset<Row> readBatch() {
        Iterator<org.apache.poi.ss.usermodel.Row> iterator = sheet.rowIterator();
        List<Row> batch = new ArrayList<>();
        int size = 1;
        while (iterator.hasNext() && batch.size() < batchSize) {
            List<String> cells = new ArrayList<>();
            iterator.next().iterator().forEachRemaining(c -> cells.add(c.getStringCellValue()));
            batch.add(RowFactory.create(cells.toArray()));
            size = cells.size();
        }
        SQLContext context = SparkSession.getActiveSession().get().sqlContext();
        return context.createDataFrame(
                batch, getSchema(size)
        );
    }

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
