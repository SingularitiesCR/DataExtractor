package com.singularities.dataextractor.cloudwriters;

import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import java.io.Serializable;

/**
 * Created by aleph on 3/12/18.
 * Singularities
 */
public class GoogleRestoreInfo implements Serializable {
  private RestorableState<WriteChannel> capture;
  private String restore_filename;
  private int restore_offset;
  private String uuid;

  public GoogleRestoreInfo() {
  }

  public GoogleRestoreInfo(RestorableState<WriteChannel> capture, String restore_filename, int
      restore_offset, String uuid) {
    this.capture = capture;
    this.restore_filename = restore_filename;
    this.restore_offset = restore_offset;
    this.uuid = uuid;
  }

  public RestorableState<WriteChannel> getCapture() {
    return capture;
  }

  public void setCapture(RestorableState<WriteChannel> capture) {
    this.capture = capture;
  }

  public String getRestore_filename() {
    return restore_filename;
  }

  public void setRestore_filename(String restore_filename) {
    this.restore_filename = restore_filename;
  }

  public int getRestore_offset() {
    return restore_offset;
  }

  public void setRestore_offset(int restore_offset) {
    this.restore_offset = restore_offset;
  }

  public void incRestore_offset() {
    this.restore_offset++;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }
}
