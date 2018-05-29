package com.singularities.dataextractor.control.parser.model;

import com.singularities.dataextractor.control.model.Config;

public interface ParserConfig {
    Config createConfig() throws Exception;
}
