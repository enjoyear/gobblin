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
import com.google.api.ads.adwords.axis.utils.v201806.SelectorBuilder;
import com.google.api.ads.adwords.axis.v201806.mcm.ManagedCustomer;
import com.google.api.ads.adwords.axis.v201806.mcm.ManagedCustomerPage;
import com.google.api.ads.adwords.axis.v201806.mcm.ManagedCustomerServiceInterface;
import com.google.api.ads.adwords.lib.client.AdWordsSession;
import com.google.api.ads.adwords.lib.selectorfields.v201806.cm.ManagedCustomerField;
import com.google.api.ads.common.lib.exception.ValidationException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class GoogleAdWordsAccountManager {
  private static final int PAGE_SIZE = 500;
  private AdWordsSession _rootSession;

  public GoogleAdWordsAccountManager(AdWordsSession rootSession) {
    _rootSession = rootSession;
  }

  /**
   *
   * @param customerId This parent customer will be included in the result set.
   * @param includeManagers Manager accounts cannot have reports. Filter managers out by setting this to false.
   */
  public Map<Long, ManagedCustomer> getChildrenAccounts(String customerId, boolean includeManagers)
      throws ValidationException, RemoteException {
    AdWordsSession.ImmutableAdWordsSession session =
        _rootSession.newBuilder().withClientCustomerId(customerId).buildImmutable();

    AdWordsServices adWordsServices = new AdWordsServices();

    ManagedCustomerServiceInterface managedCustomerService =
        adWordsServices.get(session, ManagedCustomerServiceInterface.class);

    SelectorBuilder selectorBuilder =
        new SelectorBuilder().fields(ManagedCustomerField.CustomerId, ManagedCustomerField.Name).limit(PAGE_SIZE)
            .offset(0);

    if (!includeManagers) {
      selectorBuilder = selectorBuilder.equals(ManagedCustomerField.CanManageClients, "false");
    }

    ManagedCustomerPage msp;
    int offset = 0;
    Map<Long, ManagedCustomer> managedCustomers = new HashMap<Long, ManagedCustomer>();
    do {
      selectorBuilder.offset(offset);
      msp = managedCustomerService.get(selectorBuilder.build());
      ManagedCustomer[] entries = msp.getEntries();
      if (entries != null) {
        for (ManagedCustomer managedCustomer : entries) {
          managedCustomers.put(managedCustomer.getCustomerId(), managedCustomer);
        }
      }
      offset += PAGE_SIZE;
    } while (offset < msp.getTotalNumEntries());

    return managedCustomers;
  }
}
