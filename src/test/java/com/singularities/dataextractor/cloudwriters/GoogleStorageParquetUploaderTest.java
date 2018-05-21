package com.singularities.dataextractor.cloudwriters;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by aleph on 3/12/18.
 * Singularities
 */
class GoogleStorageParquetUploaderTest {

  public static final String CREDENTAIL_FILE = "CREDENTIAL_FILE";
  public static final String BUCKET_NAME = "BUCKET_NAME";
  public static final String TEMP_TEST = "TEMP_TEST";

  public static final String TEST_UPLOAD_FILE = "TEST_UPLOAD_FILE.bin";
  public static final String TEST_UPLOAD_FOLDER = "TEST_UPLOAD_FOLDER";

  private static GoogleStorageParquetUploader uploader;
  private static Storage storage;
  private static String bucketName;

  @BeforeAll
  public static void setup() throws IOException {
    String credentialPath = System.getenv(CREDENTAIL_FILE);
    assertNotNull(credentialPath);
    bucketName = System.getenv(BUCKET_NAME);
    assertNotNull(bucketName);
    uploader = new GoogleStorageParquetUploader(bucketName, credentialPath, false);


    storage = StorageOptions.newBuilder()
        .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(credentialPath)))
        .build()
        .getService();


  }

  @Test
  void upload() throws IOException {
    String suffix = UUID.randomUUID().toString();
    File tempFile = File.createTempFile(TEMP_TEST, suffix);
    FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
    Random random = new Random();
    byte[] buffer = new byte[1024 * 1024];
    for (int i = 0; i < 10; i++) {
      random.nextBytes(buffer);
      fileOutputStream.write(buffer);
    }
    fileOutputStream.close();

    boolean upload = uploader.upload(tempFile.getAbsolutePath(), TEST_UPLOAD_FILE);
    assertTrue(upload);

    BlobId blobId = BlobId.of(bucketName, TEST_UPLOAD_FILE);
    Blob blob = storage.get(blobId);
    assertNotNull(blob);
    storage.delete(blobId);

  }

  @Test
  void uploadFolder() throws IOException {
    String prefix = UUID.randomUUID().toString();
    Path tempDirectory = Files.createTempDirectory(prefix);
    for (int iFile = 0; iFile < 5; iFile++) {
      Path tempFile = Paths.get(tempDirectory.toString(), String.format("f%d.bin", iFile));
      FileOutputStream fileOutputStream = new FileOutputStream(tempFile.toFile());
      Random random = new Random();
      byte[] buffer = new byte[1024 * 1024];
      for (int i = 0; i < 10; i++) {
        random.nextBytes(buffer);
        fileOutputStream.write(buffer);
      }
      fileOutputStream.close();
    }
    boolean upload = uploader.uploadFolder(tempDirectory.toAbsolutePath().toString(), TEST_UPLOAD_FOLDER);
    assertTrue(upload);

    ArrayList<BlobId> blobIds = new ArrayList<>(5);
    for (int i = 0; i < 5; i++) {
      blobIds.add(BlobId.of(bucketName, String.format("%s/f%d.bin", TEST_UPLOAD_FOLDER, i)));
    }
    List<Blob> blobs = storage.get(blobIds);
    assertNotNull(blobs);
    for (Blob blob : blobs) {
      assertNotNull(blob);
      storage.delete(blob.getBlobId());
    }
  }
}