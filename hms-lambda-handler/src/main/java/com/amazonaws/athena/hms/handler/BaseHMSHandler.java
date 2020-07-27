/*-
 * #%L
 * hms-lambda-handler
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.hms.handler;

// import com.amazonaws.athena.hms.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

public abstract class BaseHMSHandler<REQUEST, RESPONSE> implements RequestHandler<REQUEST, RESPONSE>
{
  // hive metastore configuration
  private final HiveMetaStoreConf conf;
  // hive metastore client
  private final HiveMetaStoreClient client;

  public BaseHMSHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    this.conf = conf;
    this.client = client;
  }

  public HiveMetaStoreConf getConf()
  {
    return conf;
  }

  public HiveMetaStoreClient getClient()
  {
    return client;
  }

  public TProtocolFactory getTProtocolFactory()
  {
    return new TJSONProtocol.Factory();
  }

  @Override
  public abstract RESPONSE handleRequest(REQUEST request, Context context);
}