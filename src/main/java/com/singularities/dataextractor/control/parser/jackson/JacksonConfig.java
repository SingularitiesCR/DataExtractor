package com.singularities.dataextractor.control.parser.jackson;

import com.singularities.dataextractor.control.model.Config;
import com.singularities.dataextractor.control.model.Job;
import com.singularities.dataextractor.control.parser.model.ParserConfig;

import java.util.ArrayList;
import java.util.List;

public class JacksonConfig implements ParserConfig {
  List<JacksonJob> jobs;

  public JacksonConfig() {
    jobs = null;
  }

  public List<JacksonJob> getJobs() {
    return jobs;
  }

  public void setJobs(List<JacksonJob> jobs) {
    this.jobs = jobs;
  }


  @Override
  public Config createConfig() throws Exception {
    if (jobs == null){
      throw new IllegalArgumentException("Jobs are required");
    }
    List<Job> acc = new ArrayList<>(this.jobs.size());
    for (JacksonJob job : jobs) {
      acc.add(job.createJob());
    }
    return new Config(acc);
  }
}
