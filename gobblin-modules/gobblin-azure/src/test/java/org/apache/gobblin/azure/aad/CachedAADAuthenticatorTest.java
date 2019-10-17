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

package org.apache.gobblin.azure.aad;

import com.microsoft.aad.adal4j.AuthenticationResult;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class CachedAADAuthenticatorTest {
  private final static String url1 = "url1";
  private final static String targetResource1 = "tr1";
  private final static String targetResource2 = "tr2";
  private final static String sp1 = "sp1";
  private final static String sp2 = "sp2";
  private final static String sp1Secret = "sp1-secret";
  private final static String sp2Secret = "sp2-secret";
  //This token expires in 0.5 second
  private final static AuthenticationResult url1s1sp1token1 = new AuthenticationResult("tokenType", "accessToken", "refreshToken",
      500, "idToken", null, false);
  private final static AuthenticationResult url1s1sp1token2 = new AuthenticationResult("tokenType", "accessToken2", "refreshToken",
      500, "idToken", null, false);
//  @Before
//  public void init() {
//    url1s1sp1token =
//  }

  @Test
  public void testGetTokenInCache() throws Exception {
    AADTokenRequester mock = new AADTokenRequesterMock();
    CachedAADAuthenticator authenticator = new CachedAADAuthenticator(mock, url1);

    Assert.assertEquals(authenticator.getToken(targetResource1, sp1, sp1Secret), url1s1sp1token1);
    Assert.assertEquals(authenticator.getToken(targetResource1, sp2, sp2Secret), null);
    Assert.assertEquals(authenticator.getToken(targetResource1, sp1, sp1Secret), url1s1sp1token1);
    Thread.sleep(1000);
    //Previous token for SP1 has expired, will fetch the new token for SP1
    Assert.assertEquals(authenticator.getToken(targetResource1, sp1, sp1Secret), url1s1sp1token2);
  }

  static class AADTokenRequesterMock implements AADTokenRequester {
    int sp1Token = 1;

    @Override
    public AuthenticationResult getToken(CachedAADAuthenticator.CacheKey key) {
      if (key.equals(new CachedAADAuthenticator.CacheKey(url1, targetResource1, sp1, sp1Secret))) {
        if (sp1Token == 1) {
          ++sp1Token;
          return url1s1sp1token1; //return url1s1sp1token1 for the first time.
        }
        //return url1s1sp1token2 later
        return url1s1sp1token2;
      }
      return null;
    }
  }
}
