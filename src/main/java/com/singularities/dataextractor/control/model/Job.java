package com.singularities.dataextractor.control.model;

import com.singularities.dataextractor.cloudwriters.CloudWriter;
import com.singularities.dataextractor.extractors.Extractor;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Job {
  private final CloudWriter writer;
  private final Extractor extractor;
  private final String name;
  private final String path;
  private final Path tempDirectory;
  private final SparkSession sparkSession;


  public Job(CloudWriter writer, Extractor extractor, String name, String path) throws IOException {
    sparkSession = SparkSession.builder().getOrCreate();
    this.writer = writer;
    this.extractor = extractor;
    this.name = name;
    this.path = path;
    String prefix = UUID.randomUUID().toString();
    tempDirectory = Files.createTempDirectory(prefix);
  }

  private String makePathPart(int current){
    return Paths.get(tempDirectory.toString(), String.format("%s.parquet.%d.part", name, current)).toString();
  }
  private String makePath(){
    return Paths.get(tempDirectory.toString(), String.format("%s.parquet", name)).toString();
  }

  public void work() throws Exception {
    int currentPart = 0;
    List<String> partList = new ArrayList<>();
    while (extractor.hasNext()){
      System.out.println(currentPart);
      String path = makePathPart(currentPart);
      partList.add(path);




      extractor.nextBatch().write().parquet(path);
      currentPart++;
    }
    int size = partList.size();
    String[] parts = new String[size];
    for (int i = 0; i < size; i++) {
      parts[i] = partList.get(i);
    }
    String path = makePath();
    sparkSession.read().parquet(parts).write().parquet(path);
    writer.uploadFolder(path, String.format("%s/%s", this.path, name));
  }

  public void clear() throws IOException {
    FileUtils.deleteDirectory(tempDirectory.toFile());
  }
}
