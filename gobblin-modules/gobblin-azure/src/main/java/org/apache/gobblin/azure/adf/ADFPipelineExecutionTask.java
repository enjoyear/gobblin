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

package org.apache.gobblin.azure.adf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Closer;
import com.microsoft.aad.adal4j.AuthenticationResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.azure.aad.AADTokenRequesterImpl;
import org.apache.gobblin.azure.aad.CachedAADAuthenticator;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.task.BaseAbstractTask;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;


/**
 * A task that execute Azure Data Factory pipelines through REST calls
 */
@Alpha
@Slf4j
public class ADFPipelineExecutionTask extends BaseAbstractTask {
  private final TaskContext taskContext;
  private final Closer closer;

  public ADFPipelineExecutionTask(TaskContext taskContext) {
    super(taskContext);
    this.taskContext = taskContext;
    this.closer = Closer.create();
  }

  @Override
  public void run() {
    TaskState taskState = this.taskContext.getTaskState();
    WorkUnit wu = taskState.getWorkunit();

    String subscriptionId = wu.getProp(ADFConfKeys.AZURE_SUBSCRIPTION_ID);
    String aadId = wu.getProp(ADFConfKeys.AZURE_ACTIVE_DIRECTORY_ID);
    String spAdfExeId = wu.getProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_ADF_EXECUTOR_ID);
    String spAdfExeSecret = "Xd*.fLzm227Awk*_ROglOSE7A8gJ/z:Y";
//    String spAKVReaderId = wu.getProp(ADFConfKeys.AZURE_SERVICE_PRINCIPAL_KEY_VAULT_READER_ID);
//    String spAKVReaderSecret = "Xd*.fLzm227Awk*_ROglOSE7A8gJ/z:Y";

    String resourceGroupName = wu.getProp(ADFConfKeys.AZURE_RESOURCE_GROUP_NAME);
    String dataFactoryName = wu.getProp(ADFConfKeys.AZURE_DATA_FACTORY_NAME);
    String apiVersion = wu.getProp(ADFConfKeys.AZURE_DATA_FACTORY_API_VERSION);
    String pipelineName = wu.getProp(ADFConfKeys.AZURE_DATA_FACTORY_PIPELINE_NAME);

    String factoryManagementUri = getDataFactoryEndpointURL(subscriptionId, resourceGroupName, dataFactoryName);
    String actionUriTemplate = factoryManagementUri + "/%s?api-version=" + apiVersion;


    AuthenticationResult token;
    try {
      CachedAADAuthenticator cachedAADAuthenticator = CachedAADAuthenticator.buildWithAADId(aadId);
      token = cachedAADAuthenticator.getToken(AADTokenRequesterImpl.TOKEN_TARGET_RESOURCE_MANAGEMENT, spAdfExeId, spAdfExeSecret);
    } catch (Exception e) {
      this.workingState = WorkUnitState.WorkingState.FAILED;
      throw new RuntimeException(e);
    }

    HttpClient httpclient = HttpClients.createDefault();
    try {
      HttpUriRequest request = executePipeline(actionUriTemplate, token, pipelineName);

      HttpResponse response = httpclient.execute(request);
      HttpEntity entity = response.getEntity();

      if (entity != null) {
        System.out.println(EntityUtils.toString(entity));
      }

      this.workingState = WorkUnitState.WorkingState.SUCCESSFUL;
      //TODO!!
      //service.shutdown(); //service needs to be shutdown, otherwise it won't exit
    } catch (Exception e) {
      log.error("ADF pipeline execution failed with error message: " + e.getMessage());
      throw new RuntimeException(e);
    }

  }

  /**
   * execute a pipeline with JSON input
   */
  private HttpUriRequest executePipeline(String actionUriTemplate, AuthenticationResult token, String pipelineName) throws URISyntaxException, IOException {
    Map<String, String> body = new HashMap<>();
    body.put("date", "2019-10-15");
    body.put("customerName", "aigupta+dev@heighten.com");
    body.put("password", "qwert12345!");
    body.put("securityToken", "a5821XsirSW24zTqqk2GzsEx");

    String action = String.format("pipelines/%s/createRun", pipelineName);
    URIBuilder pipelineExecutionBuilder = new URIBuilder(buildPipelineActionUri(actionUriTemplate, action));
    log.debug("Built Pipeline Execution URL: " + pipelineExecutionBuilder.toString());
    URI uri = pipelineExecutionBuilder.build();
    return createPostRequest(new ObjectMapper().writeValueAsString(body), token, uri);
  }

  private HttpUriRequest createPostRequest(String bodyJson, AuthenticationResult token, URI uri) throws IOException {
    HttpPost request = new HttpPost(uri);
    request.setHeader("Content-Type", "application/json"); //request
    request.setHeader("Accept", "application/json"); //response
    request.setHeader("Authorization", "Bearer " + token.getAccessToken());
    // Request body
    log.debug("Json Payload: " + bodyJson);
    StringEntity reqEntity = new StringEntity(bodyJson);
    request.setEntity(reqEntity);
    return request;
  }

  private HttpUriRequest createGetRequest(AuthenticationResult token, URI uri) throws IOException {
    HttpGet request = new HttpGet(uri);
    request.setHeader("Content-Type", "application/json"); //request
    request.setHeader("Accept", "application/json"); //response
    request.setHeader("Authorization", "Bearer " + token.getAccessToken());
    return request;
  }

  public static String getResourceGroupEndpointURL(String subscriptionId, String resourceGroupId) {
    return String.format("https://management.azure.com/subscriptions/%s/resourceGroups/%s", subscriptionId, resourceGroupId);
  }

  public static String getDataFactoryEndpointURL(String subscriptionId, String resourceGroupId, String dataFactoryName) {
    return String.format("%s/providers/Microsoft.DataFactory/factories/%s", getResourceGroupEndpointURL(subscriptionId, resourceGroupId), dataFactoryName);
  }

  //The template url is
  // https://management.azure.com/subscriptions/id/resourceGroups/rg/providers/Microsoft.DataFactory/factories/name/<xxx>?api-version=2018-06-01
  private String buildPipelineActionUri(String actionUriTemplate, String action) {
    return String.format(actionUriTemplate, action);
  }

}
