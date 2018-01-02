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
package org.apache.gobblin.converter.csv;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.math.BigDecimal;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;


/**
 * This converter simply converts double string than ends with % to a validate double number.
 * e.g. 99% -> 0.99
 */
public class PercentageDoubleStringConverter extends Converter<JsonArray, JsonArray, JsonObject, JsonObject> {
  private static final String JSON_KEY_COLUMN_NAME = "columnName";
  private static final String JSON_KEY_DATA_TYPE = "dataType";
  private static final String JSON_KEY_TYPE = "type";

  @Override
  public Converter<JsonArray, JsonArray, JsonObject, JsonObject> init(WorkUnitState workUnit) {
    super.init(workUnit);
    return this;
  }

  @Override
  public JsonArray convertSchema(JsonArray inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<JsonObject> convertRecord(JsonArray outputSchema, JsonObject inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    JsonObject outputRecord = new JsonObject();
    for (int i = 0; i < outputSchema.size(); i++) {
      JsonElement schemaElement = outputSchema.get(i);
      String col = getColumnName(schemaElement);
      JsonElement record = inputRecord.get(col);

      if ("double".equals(getTypeName(schemaElement))) {
        String recordString = record.getAsString().trim();
        if (recordString.endsWith("%")) {
          outputRecord.addProperty(col, convertPercentage(recordString));
          continue;
        }
      }
      outputRecord.add(col, record);
    }

    return new SingleRecordIterable<>(outputRecord);
  }

  private String convertPercentage(String record) {
    BigDecimal d = new BigDecimal(record.substring(0, record.length() - 1));
    return d.divide(BigDecimal.valueOf(100)).toString();
  }

  private static String getColumnName(JsonElement schemaElement) {
    return schemaElement.getAsJsonObject().get(JSON_KEY_COLUMN_NAME).getAsString();
  }

  private static String getTypeName(JsonElement schemaElement) {
    return schemaElement.getAsJsonObject().get(JSON_KEY_DATA_TYPE).getAsJsonObject().get(JSON_KEY_TYPE).getAsString()
        .toLowerCase();
  }
}
