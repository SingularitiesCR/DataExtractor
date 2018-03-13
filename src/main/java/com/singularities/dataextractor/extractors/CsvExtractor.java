package com.singularities.dataextractor.extractors;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CsvExtractor extends LineReaderExtractor {

    // Estimated bytes per line
    protected final int DEFAULT_LINE_SIZE = 80;
    private BufferedReader reader;
    private String separator = ",";
    private String next;


    public CsvExtractor() {}

    public CsvExtractor(String filename, boolean headers) throws FileNotFoundException {
        load(filename, headers);
    }

    public void load(String filename, boolean headers) throws FileNotFoundException {
        reader = new BufferedReader(new FileReader(filename), DEFAULT_LINE_SIZE * batchSize);
        try {
            next = reader.readLine();
            if (headers) {
                schema = new StructType(Lists.newArrayList(next.split(separator)).stream()
                        .map(c -> new StructField(c, DataTypes.StringType, false, Metadata.empty()))
                        .toArray(StructField[]::new));
                next = reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        return !Strings.isNullOrEmpty(next);
    }

    @Override
    public Row readNext() {
        Row toReturn = RowFactory.create((Object[]) next.split(separator));
        rowOffset++;
        try {
            next = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return toReturn;
    }

}
