package com.singularities.dataextractor.control.parser.jackson;

import com.singularities.dataextractor.control.model.Job;
import com.singularities.dataextractor.control.parser.model.ParserExtractor;
import com.singularities.dataextractor.control.parser.model.ParserJob;
import com.singularities.dataextractor.control.parser.model.ParserWriter;

public class JacksonJob implements ParserJob{
  private ParserExtractor extractor;
  private ParserWriter writer;
  private String name;
  private String path;

  public JacksonJob() {
    extractor = null;
    writer = null;
    name = null;
    path = null;
  }

  public ParserExtractor getExtractor() {
    return extractor;
  }

  public void setExtractor(ParserExtractor extractor) {
    this.extractor = extractor;
  }

  public ParserWriter getWriter() {
    return writer;
  }

  public void setWriter(ParserWriter writer) {
    this.writer = writer;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  @Override
  public Job createJob() throws Exception {
    if (writer == null){
      throw new IllegalArgumentException("Writer is required");
    }
    if (extractor == null){
      throw new IllegalArgumentException("Extractor is required");
    }
    if (name == null){
      throw new IllegalArgumentException("Name is required");
    }
    if (path == null){
      throw new IllegalArgumentException("Path is required");
    }
    return new Job(this.writer.createWriter(), this.extractor.createExtractor(), this.name, this.path);
  }
}
