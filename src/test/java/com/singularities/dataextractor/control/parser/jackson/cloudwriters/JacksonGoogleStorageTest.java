package com.singularities.dataextractor.control.parser.jackson.cloudwriters;

import com.singularities.dataextractor.cloudwriters.GoogleStorageParquetUploader;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class JacksonGoogleStorageTest {

  public static final String CREDENTAIL_FILE = "CREDENTIAL_FILE";
  public static final String BUCKET_NAME = "BUCKET_NAME";

  @Test
  void createWriter() throws IOException {
    String credentialPath = System.getenv(CREDENTAIL_FILE);
    String bucketName = System.getenv(BUCKET_NAME);
    assertNotNull(credentialPath);
    assertNotNull(bucketName);
    JacksonGoogleStorage jacksonGoogleStorage = new JacksonGoogleStorage();
    jacksonGoogleStorage.setBucketName(bucketName);
    jacksonGoogleStorage.setCredentialsFileLocation(credentialPath);
    GoogleStorageParquetUploader writer = jacksonGoogleStorage.createWriter();
//    GoogleStorageParquetUploader expected = new GoogleStorageParquetUploader(bucketName, credentialPath);
//    assertEquals(expected, writer); Google Object does not implement equals
    assertNotNull(writer);
  }
}