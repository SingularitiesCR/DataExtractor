package com.singularities.dataextractor.control.parser.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.singularities.dataextractor.cloudwriters.CloudWriter;
import com.singularities.dataextractor.control.parser.jackson.cloudwriters.JacksonGoogleStorage;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = JacksonGoogleStorage.class, name = "gcloud") })
public interface ParserWriter {
  CloudWriter createWriter() throws Exception;
}
