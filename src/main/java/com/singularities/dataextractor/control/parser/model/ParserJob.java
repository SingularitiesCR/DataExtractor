package com.singularities.dataextractor.control.parser.model;

import com.singularities.dataextractor.control.model.Job;

public interface ParserJob {
  Job createJob() throws Exception;
}
