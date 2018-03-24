package com.singularities.dataextractor.control.model;

import java.util.List;

public class Config {
  private List<Job> jobs;

  public Config(List<Job> jobs) {
    this.jobs = jobs;
  }

  public void runJobs() throws Exception {
    for (Job job : jobs) {
      job.work();
      job.clear();
    }
  }
}
