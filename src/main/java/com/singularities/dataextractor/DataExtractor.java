package com.singularities.dataextractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.singularities.dataextractor.control.model.Config;
import com.singularities.dataextractor.control.parser.model.ParserConfig;

import java.io.File;

public final class DataExtractor {

  public static void main(String[] args) throws Exception {
    if (args.length != 2){
      throw new IllegalArgumentException("Format and Path are required");
    }
    String format = args[0].toLowerCase();
    ObjectMapper mapper;

    switch (format) {
      case "json":
        mapper = new ObjectMapper();
        break;
      case "yaml":
        mapper = new ObjectMapper(new YAMLFactory());
        break;
        default:
          throw new IllegalArgumentException("Format is not valid");
    }
    File file = new File(args[1]);
    ParserConfig parserConfig = mapper.readValue(file, ParserConfig.class);
    Config config = parserConfig.createConfig();
    config.runJobs();
  }
}
