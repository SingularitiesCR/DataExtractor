package com.singularities.dataextractor.control.parser.model;

import com.singularities.dataextractor.cloudwriters.CloudWriter;

public interface ParserWriter {
  CloudWriter createWriter() throws Exception;
}
