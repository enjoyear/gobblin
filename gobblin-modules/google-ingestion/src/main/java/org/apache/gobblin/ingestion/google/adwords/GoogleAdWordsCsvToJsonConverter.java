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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Slightly changed from CsvToJsonConverterV2
 */
public class GoogleAdWordsCsvToJsonConverter extends Converter<String, JsonArray, String[], JsonObject>  {

  private static final Logger LOG = LoggerFactory.getLogger(GoogleAdWordsCsvToJsonConverter.class);
  private static final JsonParser JSON_PARSER = new JsonParser();
  private static final String COLUMN_NAME_KEY = "columnName";
  private static final String DATA_TYPE_KEY = "dataType";
  private static final String TYPE = "type";
  private static final String JSON_NULL_VAL = "null";

  public static final String CUSTOM_ORDERING = "converter.csv_to_json.custom_order";

  private List<String> customOrder;
  @Override
  public Converter<String, JsonArray, String[], JsonObject> init(WorkUnitState workUnit) {
    super.init(workUnit);
    customOrder = workUnit.getPropAsList(CUSTOM_ORDERING, "");
    if (!customOrder.isEmpty()) {
      LOG.info("Will use custom order to generate JSON from CSV: " + customOrder);
    }
    return this;
  }

  @Override
  public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    Preconditions.checkNotNull(inputSchema, "inputSchema is required.");
    return JSON_PARSER.parse(inputSchema).getAsJsonArray();
  }

  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, String[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    JsonObject outputRecord = null;
    if (!customOrder.isEmpty()) {
      outputRecord = createOutput(outputSchema, inputRecord, customOrder);
    } else {
      outputRecord = createOutput(outputSchema, inputRecord);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Converted into " + outputRecord);
    }
    return new SingleRecordIterable<JsonObject>(outputRecord);
  }

  @VisibleForTesting
  JsonObject createOutput(JsonArray outputSchema, String[] inputRecord) {
    Preconditions.checkArgument(outputSchema.size() == inputRecord.length, "# of columns mismatch. Input "
        + inputRecord.length + " , output: " + outputSchema.size());
    JsonObject outputRecord = new JsonObject();

    for (int i = 0; i < outputSchema.size(); i++) {
      JsonObject field = outputSchema.get(i).getAsJsonObject();
      String key = field.get(COLUMN_NAME_KEY).getAsString();

      if (StringUtils.isEmpty(inputRecord[i]) || JSON_NULL_VAL.equalsIgnoreCase(inputRecord[i])) {
        outputRecord.add(key, JsonNull.INSTANCE);
      } else {
        outputRecord.add(key, convertValue(inputRecord[i], field.getAsJsonObject(DATA_TYPE_KEY)));
      }
    }

    return outputRecord;
  }

  @VisibleForTesting
  JsonObject createOutput(JsonArray outputSchema, String[] inputRecord, List<String> customOrder) {

    Preconditions.checkArgument(outputSchema.size() == customOrder.size(), "# of columns mismatch. Input "
        + outputSchema.size() + " , output: " + customOrder.size());
    JsonObject outputRecord = new JsonObject();
    Iterator<JsonElement> outputSchemaIterator = outputSchema.iterator();
    Iterator<String> customOrderIterator = customOrder.iterator();

    while(outputSchemaIterator.hasNext() && customOrderIterator.hasNext()) {
      JsonObject field = outputSchemaIterator.next().getAsJsonObject();
      String key = field.get(COLUMN_NAME_KEY).getAsString();
      int i = Integer.parseInt(customOrderIterator.next());
      Preconditions.checkArgument(i < inputRecord.length, "Index out of bound detected in customer order. Index: " + i + " , # of CSV columns: " + inputRecord.length);
      if (i < 0 || null == inputRecord[i] || JSON_NULL_VAL.equalsIgnoreCase(inputRecord[i])) {
        outputRecord.add(key, JsonNull.INSTANCE);
        continue;
      }
      outputRecord.add(key, convertValue(inputRecord[i], field.getAsJsonObject(DATA_TYPE_KEY)));
    }

    return outputRecord;
  }

  /**
   * Convert string value to the expected type
   */
  private JsonElement convertValue(String value, JsonObject dataType) {
    if (dataType == null || !dataType.has(TYPE)) {
      return new JsonPrimitive(value);
    }

    String type = dataType.get(TYPE).getAsString().toUpperCase();
    ValueType valueType = ValueType.valueOf(type);
    return valueType.convert(value);
  }

  /**
   * An enum of type conversions from string value
   */
  private enum ValueType {
    INT {
      @Override
      JsonElement convert(String value) {
        return new JsonPrimitive(Double.valueOf(value).intValue());
      }
    },
    LONG {
      @Override
      JsonElement convert(String value) {
        return new JsonPrimitive(Double.valueOf(value).longValue());
      }
    },
    FLOAT {
      @Override
      JsonElement convert(String value) {
        return new JsonPrimitive(Double.valueOf(value).floatValue());
      }
    },
    DOUBLE {
      @Override
      JsonElement convert(String value) {
        value = value.trim();
        if(value.endsWith("%")){
          return new JsonPrimitive(Double.valueOf(value.substring(0, value.length() -1)));
        }
        return new JsonPrimitive(Double.valueOf(value));
      }
    },
    BOOLEAN {
      @Override
      JsonElement convert(String value) {
        return new JsonPrimitive(Boolean.valueOf(value));
      }
    },
    STRING {
      @Override
      JsonElement convert(String value) {
        return new JsonPrimitive(value);
      }
    },
    DATE {
      @Override
      JsonElement convert(String value) {
        return new JsonPrimitive(value);
      }
    }
    ;
    abstract JsonElement convert(String value);
  }
}
