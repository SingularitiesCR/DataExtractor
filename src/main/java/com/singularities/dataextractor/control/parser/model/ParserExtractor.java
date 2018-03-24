package com.singularities.dataextractor.control.parser.model;

import com.singularities.dataextractor.extractors.Extractor;

public interface ParserExtractor {
  Extractor createExtractor() throws Exception;
}
