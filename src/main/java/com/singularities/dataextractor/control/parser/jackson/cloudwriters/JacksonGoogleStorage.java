package com.singularities.dataextractor.control.parser.jackson.cloudwriters;

import com.singularities.dataextractor.cloudwriters.GoogleStorageParquetUploader;
import com.singularities.dataextractor.control.parser.model.ParserWriter;

import java.io.IOException;

public class JacksonGoogleStorage implements ParserWriter {
  private String bucketName;
  private String credentialsFileLocation;
  private boolean overwrite;

  public JacksonGoogleStorage() {
    bucketName = null;
    credentialsFileLocation = null;
    overwrite = false;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public String getCredentialsFileLocation() {
    return credentialsFileLocation;
  }

  public void setCredentialsFileLocation(String credentialsFileLocation) {
    this.credentialsFileLocation = credentialsFileLocation;
  }

  public GoogleStorageParquetUploader createWriter() throws IOException {
    if (bucketName == null){
      throw new IllegalArgumentException("Bucket name is required");
    }
    if (credentialsFileLocation == null){
      throw new IllegalArgumentException("Credential File Location is required");
    }
    return new GoogleStorageParquetUploader(bucketName, credentialsFileLocation, overwrite);
  }
}
