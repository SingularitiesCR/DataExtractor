package com.singularities.dataextractor.control.parser.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.singularities.dataextractor.control.parser.jackson.extractors.JacksonCsv;
import com.singularities.dataextractor.control.parser.jackson.extractors.JacksonSql;
import com.singularities.dataextractor.control.parser.jackson.extractors.JacksonXlsx;
import com.singularities.dataextractor.extractors.Extractor;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = JacksonCsv.class, name = "csv"),
        @JsonSubTypes.Type(value = JacksonSql.class, name = "sql"),
        @JsonSubTypes.Type(value = JacksonXlsx.class, name = "xlsx")})
public interface ParserExtractor {
  Extractor createExtractor() throws Exception;
}
