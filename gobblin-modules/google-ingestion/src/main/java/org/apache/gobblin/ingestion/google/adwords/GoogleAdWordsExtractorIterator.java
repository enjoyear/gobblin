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
package org.apache.gobblin.ingestion.google.adwords;

import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.ingestion.google.AsyncIteratorWithDataSink;
import org.apache.gobblin.ingestion.google.GoggleIngestionConfigurationKeys;


@Slf4j
public class GoogleAdWordsExtractorIterator extends AsyncIteratorWithDataSink<String[]> {
  private GoogleAdWordsReportDownloader _googleAdWordsReportDownloader;
  private Collection<String> _accounts;

  public GoogleAdWordsExtractorIterator(GoogleAdWordsReportDownloader googleAdWordsReportDownloader,
      Collection<String> accounts, WorkUnitState state) {
    super(state.getPropAsInt(GoggleIngestionConfigurationKeys.SOURCE_ASYNC_ITERATOR_BLOCKING_QUEUE_SIZE, 2000),
        state.getPropAsInt(GoggleIngestionConfigurationKeys.SOURCE_ASYNC_ITERATOR_POLL_BLOCKING_TIME, 1));
    _googleAdWordsReportDownloader = googleAdWordsReportDownloader;
    _accounts = accounts;
  }

  @Override
  protected Runnable getProducerRunnable() {
    return new Runnable() {
      @Override
      public void run() {
        try {
          _googleAdWordsReportDownloader.downloadAllReports(_accounts, _dataSink);
        } catch (InterruptedException e) {
          log.error(e.getMessage());
          throw new RuntimeException(e);
        }
      }
    };
  }
}
