/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.scheduler.kafka;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.runtime.JobLauncherFactory;
import org.apache.gobblin.runtime.job_spec.JobSpecResolver;
import org.apache.gobblin.runtime.listeners.JobListener;
import org.apache.gobblin.runtime.listeners.RunOnceJobListener;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Slf4j
public class KafkaJobListenerService extends AbstractIdleService {
  // System configuration properties
  public final Properties properties;

  // A thread pool executor for running jobs without schedules
  //TODO: parameterize this and also disable(or reduces threads count to 1) the ThreadPool in SchedulerService
  protected final ExecutorService jobExecutor = Executors.newFixedThreadPool(1);

  private final Closer closer = Closer.create();

  @Getter
  private final JobSpecResolver jobSpecResolver;

  public KafkaJobListenerService(Properties properties) throws IOException {
    this.properties = properties;
    this.jobSpecResolver = JobSpecResolver.builder(ConfigUtils.propertiesToConfig(properties)).build();
  }

  @Override
  protected void startUp()
      throws Exception {
    log.info("Starting the kafka job listener");
    Properties prop = null;
    try (InputStream input = new FileInputStream("/Users/chguo/Downloads/adfPipelineExeReal.template")) {
      Config config = ConfigFactory.parseReader(new InputStreamReader(input, Charsets.UTF_8));
      prop = ConfigUtils.configToProperties(config);
    } catch (IOException ex) {
      ex.printStackTrace();
    }
    
    jobExecutor.submit(new ADFJobRunner(prop, new RunOnceJobListener()));
  }


  @Override
  protected void shutDown()
      throws Exception {
    log.info("Stopping the kafka job listener");
    closer.close();
    ExecutorsUtils.shutdownExecutorService(this.jobExecutor, Optional.of(log));
  }

  /**
   * Run a job.
   *
   * <p>
   * This method runs the job immediately without going through the Quartz scheduler.
   * This is particularly useful for testing.
   * </p>
   *
   * @param jobProps    Job configuration properties
   * @param jobListener {@link JobListener} used for callback, can be <em>null</em> if no callback is needed.
   * @throws JobException when there is anything wrong with running the job
   */
  public void runJob(Properties jobProps, JobListener jobListener) throws JobException {
    try {
      JobLauncher jobLauncher = JobLauncherFactory.newJobLauncher(this.properties, jobProps);
      runJob(jobProps, jobListener, jobLauncher);
    } catch (Exception e) {
      throw new JobException("Failed to run job " + jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), e);
    }
  }

  /**
   * Run a job.
   *
   * <p>
   * This method runs the job immediately without going through the Quartz scheduler.
   * This is particularly useful for testing.
   * </p>
   *
   * <p>
   * This method does what {@link #runJob(Properties, JobListener)} does, and additionally it allows
   * the caller to pass in a {@link JobLauncher} instance used to launch the job to run.
   * </p>
   *
   * @param jobProps    Job configuration properties
   * @param jobListener {@link JobListener} used for callback, can be <em>null</em> if no callback is needed.
   * @param jobLauncher a {@link JobLauncher} object used to launch the job to run
   * @return If current job is a stop-early job based on {@link Source#isEarlyStopped()}
   * @throws JobException when there is anything wrong with running the job
   */
  public void runJob(Properties jobProps, JobListener jobListener, JobLauncher jobLauncher)
      throws JobException {
    Preconditions.checkArgument(jobProps.containsKey(ConfigurationKeys.JOB_NAME_KEY),
        "A job must have a job name specified by job.name");
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);

    // Launch the job
    try (Closer closer = Closer.create()) {
      closer.register(jobLauncher).launchJob(jobListener);
    } catch (Throwable t) {
      throw new JobException("Failed to launch and run job " + jobName, t);
    }
  }


  class ADFJobRunner implements Runnable {

    private final Properties jobProps;
    private final JobListener jobListener;

    public ADFJobRunner(Properties jobProps, JobListener jobListener) {
      this.jobProps = jobProps;
      this.jobListener = jobListener;
    }

    @Override
    public void run() {
      try {
        //closer.register(kafkaConsumer)
        //loadGeneralJobConfigs()

        //new NonScheduledJobRunner(jobProps, jobListener)
        KafkaJobListenerService.this.runJob(jobProps, jobListener);

      } catch (JobException je) {
        log.error("Failed to run job " + this.jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY), je);
      }
    }
  }
}
