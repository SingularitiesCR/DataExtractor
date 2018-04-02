package com.singularities.dataextractor.control.parser.jackson.cloudwriters;

import com.singularities.dataextractor.cloudwriters.GoogleStorageParquetUploader;
import com.singularities.dataextractor.control.parser.jackson.extractors.JacksonCsv;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

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

  @Test
  @Ignore
  void testJsonFile() throws IOException {
    String path = Objects.requireNonNull(getClass().getClassLoader().getResource("googleStorage.json")).getPath();
    ObjectMapper mapper = new ObjectMapper();
    JacksonGoogleStorage storage;
    storage = mapper.readValue(new File(path), JacksonGoogleStorage.class);
    GoogleStorageParquetUploader writer = storage.createWriter();
//    GoogleStorageParquetUploader expected = new GoogleStorageParquetUploader(bucketName, credentialPath);
//    assertEquals(expected, writer); Google Object does not implement equals
    assertNotNull(writer);
  }

}