package com.singularities.dataextractor.cloudwriters;

import java.io.IOException;

public interface CloudWriter {

  public boolean upload(String fileLocation, String targetLocation) throws IOException;
  public boolean uploadFolder(String folderLocation, final String targetLocation) throws IOException;

}
