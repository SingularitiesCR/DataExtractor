package com.singularities.dataextractor.cloudwriters;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by aleph on 3/12/18.
 * Singularities
 */
public class GoogleStorageParquetUploader implements CloudWriter{

  private static final Logger logger = LogManager.getLogger(GoogleStorageParquetUploader.class);


  private String bucketName;
  private Storage storage;
  private boolean overwrite;

  private static final String MIME = "application/octet-stream";
  private static final int BUFFER_SIZE = 4194304; // 4 MiB (1024*1024*64 bytes)

  //restore
  private List<GoogleRestoreInfo> restoreInfo;

  public List<GoogleRestoreInfo> getRestoreInfo() {
    return restoreInfo;
  }
  /**
   * Creates a Google Storage uploader
   * @param bucketName Name of the bucket
   * @param credentialsFileLocation Location of the credential file
   * @param overwrite Write mode
   * @throws FileNotFoundException Inherit from FileInputStream
   * @throws IOException Inherit from ServiceAccountCredentials.fromStream
   */
  public GoogleStorageParquetUploader(String bucketName, String credentialsFileLocation, boolean overwrite) throws FileNotFoundException, IOException {
    this.bucketName = bucketName;
    this.overwrite = overwrite;
    this.storage = StorageOptions.newBuilder()
        .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(credentialsFileLocation)))
        .build()
        .getService();
  }

  /**
   * Upload a single file
   * @param fileLocation Path to the file
   * @param targetLocation Path to place the file in gs://bucket-name
   * @return true if success, false if recovery is possible
   * @throws IOException Inherit from BufferWriter
   */
  @Override
  public boolean upload(String fileLocation, String targetLocation) throws IOException {

    BlobId blobId = BlobId.of(this.bucketName, targetLocation);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(MIME).build();
    WriteChannel writeChannel = this.storage.writer(blobInfo);

    GoogleRestoreInfo current_restoreInfo = new GoogleRestoreInfo();
    current_restoreInfo.setUuid(UUID.randomUUID().toString());
    current_restoreInfo.setRestore_filename(fileLocation);
    current_restoreInfo.setRestore_offset(0);

    FileInputStream inputStream = new FileInputStream(fileLocation);
    byte[] buffer = new byte[BUFFER_SIZE];
    int read = inputStream.read(buffer, 0, BUFFER_SIZE);
    while (read > 0){
      try {
        writeChannel.write(ByteBuffer.wrap(buffer, 0, read));
      }
      catch (IOException e) {
        logger.error(String.format("Unable to upload part %d of file %s \nTo attempt a restore " +
                "please use the restore option with file %s", current_restoreInfo.getRestore_offset(),
            current_restoreInfo.getRestore_filename(), current_restoreInfo.getUuid()));
        restoreInfo.add(current_restoreInfo);
        return false;

      }
      current_restoreInfo.setCapture(writeChannel.capture());
      current_restoreInfo.incRestore_offset();
      Arrays.fill(buffer, (byte) 0);
      read = inputStream.read(buffer, 0, BUFFER_SIZE);
    }
    writeChannel.close();
    restoreInfo = null;
    return true;
  }

  /**
   * Upload the contents of the folder
   * @param folderLocation Path to the folder
   * @param targetLocation Path to put the folder contents
   * @return True if success, false if there is a possible recovery
   * @throws IOException Inherit from upload
   */
  @Override
  public boolean uploadFolder(String folderLocation, final String targetLocation) throws IOException {
    boolean canWrite = true;
    if (this.overwrite){
      canWrite = this.removeFolder(targetLocation);
    }
    if (canWrite){
      return Files.walk(Paths.get(folderLocation))
              .filter(path -> Files.isRegularFile(path))
              .map(fileLocation -> {
                try {
                  String location = fileLocation.toAbsolutePath().toString();
                  String name = String.format("%s/%s", targetLocation, Paths.get(folderLocation)
                          .relativize(fileLocation).toString());
                  return upload(location, name);
                } catch (IOException e) {
                  e.printStackTrace();
                  logger.error(String.format("Unable to upload %s", fileLocation));
                  return false;
                }
              }).reduce((b1, b2) -> b1 && b2).orElse(false);
    }else{
      logger.error("Unable to delete previous directory. Upload error.");
      return false;
    }

  }

  private boolean removeFolder(final String targetLocation) {
      try {
        Iterable<Blob> blobs = storage.list(this.bucketName, Storage.BlobListOption.prefix(targetLocation)).iterateAll();
        for (Blob blob : blobs) {
          blob.delete(Blob.BlobSourceOption.generationMatch());
        }
        return true;
      } catch ( Exception e){
        logger.error(e);
        logger.error(String.format("Unable to delete the file %s in bucket %s", targetLocation, this.bucketName));
        return false;
      }
  }
}
