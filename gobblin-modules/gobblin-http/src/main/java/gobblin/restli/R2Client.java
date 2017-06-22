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

package gobblin.restli;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import com.linkedin.common.callback.Callbacks;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import gobblin.async.Callback;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.http.ThrottledHttpClient;

@Slf4j
public class R2Client extends ThrottledHttpClient<RestRequest, RestResponse> {
  private final Client client;

  public R2Client(Client client, SharedResourcesBroker broker) {
    super (broker, getLimiterKey());
    this.client = client;
  }

  @Override
  public RestResponse sendRequestImpl(RestRequest request)
      throws IOException {
    Future<RestResponse> responseFuture = client.restRequest(request);
    RestResponse response;
    try {
      response = responseFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
    return response;
  }

  @Override
  public void sendAsyncRequestImpl(RestRequest request, Callback<RestResponse> callback)
      throws IOException {
    AsyncR2CallbackWrapper wrapper = new AsyncR2CallbackWrapper(callback);
    client.restRequest(request, wrapper);
  }

  @Override
  public void close()
      throws IOException {
    client.shutdown(Callbacks.<None>empty());
  }

  private static String getLimiterKey () {
    return "D2request/" + "serviceName";
  }

  /**
   * A wrapper class which passes result from {@link com.linkedin.common.callback.Callback<RestResponse>} to {@link Callback}
   */
  @Getter
  private static class AsyncR2CallbackWrapper implements com.linkedin.common.callback.Callback<RestResponse> {
    private Callback<RestResponse>  callback = null;

    public AsyncR2CallbackWrapper(Callback<RestResponse>  callback) {
      this.callback = callback;
    }

    @Override
    public void onError(Throwable e) {
      this.callback.onFailure(e);
    }

    @Override
    public void onSuccess(RestResponse result) {
      this.callback.onSuccess(result);
    }
  }

}
