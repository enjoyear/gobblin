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

import com.google.api.ads.adwords.axis.factory.AdWordsServices;
import com.google.api.ads.adwords.axis.v201806.cm.ReportDefinitionField;
import com.google.api.ads.adwords.axis.v201806.cm.ReportDefinitionServiceInterface;
import com.google.api.ads.adwords.axis.v201806.mcm.ManagedCustomer;
import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.adwords.lib.jaxb.v201806.ReportDefinitionDateRangeType;
import com.google.api.ads.adwords.lib.jaxb.v201806.ReportDefinitionReportType;
import com.google.api.ads.common.lib.exception.ValidationException;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.avro.JsonElementConversionFactory;
import org.apache.gobblin.ingestion.google.util.SchemaUtil;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.extractor.partition.Partition;
import org.apache.gobblin.source.extractor.watermark.DateWatermark;
import org.apache.gobblin.source.extractor.watermark.TimestampWatermark;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


@Slf4j
public class GoogleAdWordsExtractor implements Extractor<String, String[]> {
  private static final String ACCOUNT_ID_COLUMN = "extraAccId";
  private final DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
  private final static Splitter SPLIT_BY_COMMA = Splitter.on(",").omitEmptyStrings().trimResults();
  private final WorkUnitState _state;
  private final GoogleAdWordsExtractorIterator _iterator;
  private final DateTime _startDate;
  private final DateTime _expectedEndDate;
  private JsonArray _schema;
  private final DateTimeFormatter watermarkFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss");
  private final static Map<String, JsonElementConversionFactory.Type> TYPE_CONVERSION_MAP;

  private boolean _successful = false;

  static {
    ConcurrentHashMap<String, JsonElementConversionFactory.Type> typeMap = new ConcurrentHashMap<>();
    typeMap.put("string", JsonElementConversionFactory.Type.STRING);
    typeMap.put("integer", JsonElementConversionFactory.Type.INT);
    typeMap.put("long", JsonElementConversionFactory.Type.LONG);
    typeMap.put("float", JsonElementConversionFactory.Type.FLOAT);
    typeMap.put("double", JsonElementConversionFactory.Type.DOUBLE);
    typeMap.put("boolean", JsonElementConversionFactory.Type.BOOLEAN);
    typeMap.put("date", JsonElementConversionFactory.Type.DATE);
    TYPE_CONVERSION_MAP = Collections.unmodifiableMap(typeMap);
  }

  public GoogleAdWordsExtractor(WorkUnitState state)
      throws Exception {
    _state = state;
    Preconditions.checkArgument(
        _state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE).compareToIgnoreCase("Hour") == 0);
    Preconditions.checkArgument(_state.getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_PARTITION_INTERVAL) == 24);

    Partition partition = Partition.deserialize(_state.getWorkunit());
    long lowWatermark = partition.getLowWatermark();
    long expectedHighWatermark = partition.getHighWatermark();
    /*
      This change is needed because
      1. The partition behavior changed due to commit 7d730fcb0263b8ca820af0366818160d638d1336 [7d730fc]
       by zxcware <zxcware@gmail.com> on April 3, 2017 at 11:47:41 AM PDT
      2. Google AdWords API only cares about Dates, and are both side inclusive.
      Therefore, do the following processing.
     */
    int dateDiff = partition.isHighWatermarkInclusive() ? 1 : 0;
    long highWatermarkDate = DateWatermark.adjustWatermark(Long.toString(expectedHighWatermark), dateDiff);
    long updatedExpectedHighWatermark = TimestampWatermark.adjustWatermark(Long.toString(highWatermarkDate), -1);
    updatedExpectedHighWatermark = Math.max(lowWatermark, updatedExpectedHighWatermark);

    _startDate = watermarkFormatter.parseDateTime(Long.toString(lowWatermark));
    _expectedEndDate = watermarkFormatter.parseDateTime(Long.toString(updatedExpectedHighWatermark));

    GoogleAdWordsCredential credential = new GoogleAdWordsCredential(state);
    AdWordsSession.ImmutableAdWordsSession rootSession = credential.buildRootSession();

    ReportDefinitionReportType reportType =
        ReportDefinitionReportType.valueOf(state.getProp(GoogleAdWordsSource.KEY_REPORT).toUpperCase() + "_REPORT");
    ReportDefinitionDateRangeType dateRangeType =
        ReportDefinitionDateRangeType.valueOf(state.getProp(GoogleAdWordsSource.KEY_DATE_RANGE).toUpperCase());
    if (!dateRangeType.equals(ReportDefinitionDateRangeType.CUSTOM_DATE) && !dateRangeType
        .equals(ReportDefinitionDateRangeType.ALL_TIME)) {
      throw new UnsupportedOperationException("Only support date range of custom_date or all_time");
    }

    String columnNamesString = state.getProp(GoogleAdWordsSource.KEY_COLUMN_NAMES, "");
    List<String> columnNames =
        columnNamesString.trim().isEmpty() ? null : Lists.newArrayList(SPLIT_BY_COMMA.split(columnNamesString));

    HashMap<String, String> allFields = downloadReportFields(rootSession, reportType);
    try {
      _schema = createSchema(allFields, columnNames, _state.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, ""));
      log.info(String.format("Schema for report %s: %s", reportType, _schema));
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed downloading report %s", reportType), e);
    }

    String startDate = dateFormatter.print(_startDate);
    String endDate = dateFormatter.print(_expectedEndDate);
    GoogleAdWordsReportDownloader downloader =
        new GoogleAdWordsReportDownloader(rootSession, _state, startDate, endDate, reportType, dateRangeType, _schema);

    _iterator = new GoogleAdWordsExtractorIterator(downloader, getConfiguredAccounts(rootSession, state), state);
  }

  /**
   * 1. Get all available non-manager accounts.
   * 2. If exactAccounts are provided, validate that all exactAccounts are a subset of step 1.
   * 3. If exclusiveAccounts are provided, remove them from step 1.
   */
  static Collection<String> getConfiguredAccounts(AdWordsSession rootSession, WorkUnitState state)
      throws ValidationException, RemoteException {

    String masterCustomerId = state.getProp(GoogleAdWordsSource.KEY_MASTER_CUSTOMER);
    Optional<String> exactAccountOpt = Optional.fromNullable(state.getProp(GoogleAdWordsSource.KEY_ACCOUNTS_EXACT));
    Set<String> exactAccounts =
        exactAccountOpt.isPresent() ? Sets.newHashSet(SPLIT_BY_COMMA.split(exactAccountOpt.get())) : null;

    Optional<String> excAccountOpt = Optional.fromNullable(state.getProp(GoogleAdWordsSource.KEY_ACCOUNTS_EXCLUDE));
    Set<String> exclusiveAccounts =
        excAccountOpt.isPresent() ? Sets.newHashSet(SPLIT_BY_COMMA.split(excAccountOpt.get())) : null;

    GoogleAdWordsAccountManager accountManager = new GoogleAdWordsAccountManager(rootSession);
    Map<Long, ManagedCustomer> availableAccounts = accountManager.getChildrenAccounts(masterCustomerId, false);
    Set<String> available = new HashSet<>();
    for (Map.Entry<Long, ManagedCustomer> account : availableAccounts.entrySet()) {
      available.add(Long.toString(account.getKey()));
    }
    log.info(
        String.format("Found %d available accounts for your master account %s", available.size(), masterCustomerId));

    if (exactAccounts != null) {
      Sets.SetView<String> difference = Sets.difference(exactAccounts, available);
      if (difference.isEmpty()) {
        return exactAccounts;
      } else {
        String msg = String
            .format("The following accounts configured in the exact list don't exist under master account %s: %s",
                masterCustomerId, Joiner.on(",").join(difference));
        log.error(msg);
        throw new RuntimeException(msg);
      }
    }

    if (exclusiveAccounts != null && !exclusiveAccounts.isEmpty()) {
      available.removeAll(exclusiveAccounts);
    }
    return available;
  }

  @Override
  public String getSchema()
      throws IOException {
    JsonArray updatedSchema = new JsonArray();
    for (int i = 0; i < _schema.size(); i++) {
      updatedSchema.add(_schema.get(i));
    }
    //add extra columns(AccountId) at the end of the original schema
    updatedSchema.add(SchemaUtil.createColumnJson(ACCOUNT_ID_COLUMN, false, JsonElementConversionFactory.Type.STRING));
    return updatedSchema.toString();
  }

  @Override
  public String[] readRecord(@Deprecated String[] reuse)
      throws DataRecordException, IOException {
    while (_iterator.hasNext()) {
      return _iterator.next();
    }
    _successful = true;
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    if (_successful) {
      //Any positive number will be okay.
      //Need to add this because of this commit:
      //76ae45a by ibuenros on 12/20/16 at 11:34AM Query based source will reset to low watermark if previous run did not process any data for that table.
      return 1;
    }
    return 0;
  }

  @Override
  public long getHighWatermark() {
    throw new UnsupportedOperationException("This method has been deprecated!");
  }

  @Override
  public void close()
      throws IOException {
    if (_successful) {
      log.info(String
          .format("Successfully downloaded %s reports for [%s, %s).", _state.getProp(GoogleAdWordsSource.KEY_REPORT),
              dateFormatter.print(_startDate), dateFormatter.print(_expectedEndDate)));
      _state.setActualHighWatermark(_state.getWorkunit().getExpectedHighWatermark(LongWatermark.class));
    } else {
      log.error(String
          .format("Failed downloading %s reports for [%s, %s).", _state.getProp(GoogleAdWordsSource.KEY_REPORT),
              dateFormatter.print(_startDate), dateFormatter.print(_expectedEndDate)));
    }
  }

  static JsonArray createSchema(HashMap<String, String> allFields, List<String> requestedColumns, String deltaColumn)
      throws IOException {
    JsonArray schema = new JsonArray();
    TreeMap<String, String> selectedColumns;

    if (requestedColumns == null || requestedColumns.isEmpty()) {
      selectedColumns = new TreeMap<>(allFields);
    } else {
      selectedColumns = new TreeMap<>();
      for (String columnName : requestedColumns) {
        String type = allFields.get(columnName);
        if (type == null) {
          throw new IOException(String.format("Column %s doesn't exist", columnName));
        }
        selectedColumns.put(columnName, type);
      }
    }

    if (!Strings.isNullOrEmpty(deltaColumn)) {
      Preconditions.checkArgument(selectedColumns.containsKey(deltaColumn),
          String.format("The configured DELTA Column %s doesn't exist!", deltaColumn));
    }

    for (Map.Entry<String, String> column : selectedColumns.entrySet()) {
      String typeString = column.getValue().toLowerCase();
      JsonElementConversionFactory.Type acceptedType = TYPE_CONVERSION_MAP.get(typeString);
      if (acceptedType == null) {
        acceptedType = JsonElementConversionFactory.Type.STRING;
      }
      String columnName = column.getKey();
      if (!Strings.isNullOrEmpty(deltaColumn) && deltaColumn.equalsIgnoreCase(columnName)) {
        schema.add(SchemaUtil.createColumnJson(columnName, false, acceptedType));
      } else {
        schema.add(SchemaUtil.createColumnJson(columnName, true, acceptedType));
      }
    }
    return schema;
  }

  private static HashMap<String, String> downloadReportFields(AdWordsSession rootSession,
      ReportDefinitionReportType reportType)
      throws RemoteException {
    try {
      AdWordsServices adWordsServices = new AdWordsServices();
      ReportDefinitionServiceInterface reportDefinitionService =
          adWordsServices.get(rootSession, ReportDefinitionServiceInterface.class);

      ReportDefinitionField[] reportDefinitionFields = reportDefinitionService.getReportFields(
          com.google.api.ads.adwords.axis.v201806.cm.ReportDefinitionReportType.fromString(reportType.toString()));
      HashMap<String, String> fields = new HashMap<>();
      for (ReportDefinitionField field : reportDefinitionFields) {
        fields.put(field.getFieldName(), field.getFieldType());
      }
      return fields;
    } catch (RemoteException e) {
      log.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }
}