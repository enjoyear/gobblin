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

package org.apache.gobblin.azure.k2;

import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.azure.keyvault.models.SecretBundle;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.azure.aad.AADTokenRequesterImpl;
import org.apache.gobblin.azure.aad.CachedAADAuthenticator;
import org.apache.gobblin.azure.adf.ADFConfKeys;
import org.apache.gobblin.azure.adf.ADFPipelineExecutionTask;
import org.apache.gobblin.azure.key_vault.KeyVaultSecretRetriever;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.source.workunit.WorkUnit;

import java.util.HashMap;
import java.util.Map;

/**
 * This will be moved internally
 */
@Slf4j
public class K2ADFPipelineExecutionTask extends ADFPipelineExecutionTask {

  /**
   * The K2 Pipeline Execution Task
   */
  public K2ADFPipelineExecutionTask(TaskContext taskContext) {
    super(taskContext);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Map<String, String> providePayloads() {
    Map<String, String> body = new HashMap<>();
    body.put("date", "2019-10-15");
    body.put("customerName", "aigupta+dev@heighten.com");
    body.put("password", "qwert12345!");
    body.put("securityToken", "a5821XsirSW24zTqqk2GzsEx");
    return body;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AuthenticationResult getAuthenticationToken() {
    TaskState taskState = this.taskContext.getTaskState();
    WorkUnit wu = taskState.getWorkunit();

    String keyVaultUrl = wu.getProp(ADFConfKeys.AZURE_KEY_VAULT_URL);
    String spAkvReaderId = wu.getProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_ID);
    String spAkvReaderSecret = wu.getProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_SECRET);

    SecretBundle fetchedSecret = new KeyVaultSecretRetriever(keyVaultUrl).getSecret(spAkvReaderId, spAkvReaderSecret, "secret-spi-cdppoc-ei");

    String aadId = wu.getProp(ADFConfKeys.AZURE_ACTIVE_DIRECTORY_ID);
    String spAdfExeId = wu.getProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_ID);
    String spAdfExeSecret = fetchedSecret.value();

    AuthenticationResult token;
    try {
      CachedAADAuthenticator cachedAADAuthenticator = CachedAADAuthenticator.buildWithAADId(aadId);
      token = cachedAADAuthenticator.getToken(AADTokenRequesterImpl.TOKEN_TARGET_RESOURCE_MANAGEMENT, spAdfExeId, spAdfExeSecret);
    } catch (Exception e) {
      this.workingState = WorkUnitState.WorkingState.FAILED;
      throw new RuntimeException(e);
    }
    return token;
  }
}
