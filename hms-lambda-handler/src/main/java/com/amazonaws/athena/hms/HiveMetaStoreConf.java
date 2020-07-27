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
package com.amazonaws.athena.hms;

import com.amazonaws.athena.conf.Configuration;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.conf.HiveConf;

public class HiveMetaStoreConf
{
  public static final String HMS_PROPERTIES = "hms.properties";
  public static final String HMS_KERBEROS_ENABLED = "hive.metastore.kerberos.enabled";
  public static final String HMS_RESPONSE_SPILL_LOCATION = "hive.metastore.response.spill.location";
  public static final String HMS_RESPONSE_SPILL_THRESHOLD = "hive.metastore.response.spill.threshold";
  public static final String HMS_HANDLER_NAME_PREFIX = "hive.metastore.handler.name.prefix";
  public static final String DEFAULT_HMS_HANDLER_NAME_PREFIX = "com.amazonaws.athena.hms.handler.";
  public static final long DEFAULT_HMS_RESPONSE_SPILL_THRESHOLD = 4 * 1024 * 1024; // 4MB
  public static final String ENV_HMS_URIS = "HMS_URIS";
  public static final String ENV_SPILL_LOCATION = "SPILL_LOCATION";


  //Hive configuration "javax.jdo.option.ConnectionURL"
  private String connectionURL;

  //Hive configuration "javax.jdo.option.ConnectionDriverName"
  private String connectionDriverName;

  //Hive configuration "javax.jdo.option.ConnectionPassword"
  private String connectionPassword;

  //Hive configuration "javax.jdo.option.ConnectionUserName"
  private String connectionUserName;

  //Hive configuration "hive.metastore.warehouse.dir"
  private String metaWarehouse;

  // Hive configuration: "hive.metastore.execute.setugi"
  private boolean metastoreSetUgi;

  // Hive configuration: "hive.metastore.uris"
  private String metastoreUri;

  // the threshold to decide whether to spill the response to s3 or not
  private long responseSpillThreshold;

  // the root s3 path to store the spilled response file
  private String responseSpillLocation;

  // the handler name prefix
  private String handlerNamePrefix;

  public boolean isMetastoreSetUgi()
  {
    return metastoreSetUgi;
  }

  public void setMetastoreSetUgi(boolean metastoreSetUgi)
  {
    this.metastoreSetUgi = metastoreSetUgi;
  }

  public void setMetastoreUri(String metastoreUri)
  {
    this.metastoreUri = metastoreUri;
  }


  public void setConnectionURL(String connectionURL) { this.connectionURL = connectionURL; }

  public void setConnectionDriverName(String connectionDriverName) { this.connectionDriverName = connectionDriverName; }

  public void setConnectionUserName(String connectionUserName) { this.connectionUserName = connectionUserName; }

  public void setConnectionPassword(String connectionPassword) { this.connectionPassword = connectionPassword; }

  public void setMetastoreWarehouse(String metaWarehouse) { this.metaWarehouse = metaWarehouse; }

  public String getResponseSpillLocation()
  {
    return responseSpillLocation;
  }

  public void setResponseSpillLocation(String responseSpillLocation)
  {
    this.responseSpillLocation = responseSpillLocation;
  }

  public String getConnectionURL() { return connectionURL; }

  public String getConnectionDriverName() { return connectionDriverName; }

  public String getConnectionUserName() { return connectionUserName; }

  public String getConnectionPassword() { return connectionPassword; }

  public String getMetaWarehouse() { return metaWarehouse; }

  public long getResponseSpillThreshold()
  {
    return responseSpillThreshold;
  }

  public String getMetastoreUri()
  {
    return metastoreUri;
  }

  public void setResponseSpillThreshold(long responseSpillThreshold)
  {
    this.responseSpillThreshold = responseSpillThreshold;
  }

  public String getHandlerNamePrefix()
  {
    return handlerNamePrefix;
  }

  public void setHandlerNamePrefix(String handlerNamePrefix)
  {
    this.handlerNamePrefix = handlerNamePrefix;
  }

  /*
   * convert this configuration class to an HiveConf object
   *
   * @return HiveConf
   */
  public HiveConf toHiveConf()
  {
    HiveConf conf = new HiveConf();

    System.out.println("User name: " +connectionUserName);
    System.out.println("JDBC: " +connectionURL);
    System.out.println("Password: " +connectionPassword);
    System.out.println("Driver: " +connectionDriverName);
    System.out.println("Metastore: " +metaWarehouse);

    /* hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME,"mars");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREPWD,"Marsffhh2$68");
    hiveConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER,"org.mariadb.jdbc.Driver");
    hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
            "jdbc:mysql://hivemetadb-instance-1.cxlolvlnaden.us-east-1.rds.amazonaws.com:3306/hivedb");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE,"s3a://vasveena-test-finr/rootdb"); */

    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME, connectionUserName);
    conf.setVar(HiveConf.ConfVars.METASTOREPWD, connectionPassword);
    conf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, connectionURL);
    conf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, connectionDriverName);
    conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, metaWarehouse);

    conf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, metastoreSetUgi);

    return conf;
  }

  /*
   * load the HiveMetaStoreConf from a property file "hms.properties"
   *
   * @return HiveMetaStoreConf
   */
  public static HiveMetaStoreConf load()
  {
    Configuration hmsConf = Configuration.loadDefaultFromClasspath(HMS_PROPERTIES);

    HiveMetaStoreConf conf = new HiveMetaStoreConf();

    conf.setConnectionURL(hmsConf.getProperty(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname));
    conf.setConnectionDriverName(hmsConf.getProperty(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER.varname));
    conf.setConnectionPassword(hmsConf.getProperty(HiveConf.ConfVars.METASTOREPWD.varname));
    conf.setConnectionUserName(hmsConf.getProperty(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME.varname));
    conf.setMetastoreWarehouse(hmsConf.getProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname));

    conf.setMetastoreSetUgi(hmsConf.getBoolean(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI.varname, true));
    conf.setResponseSpillLocation(hmsConf.getProperty(HMS_RESPONSE_SPILL_LOCATION));
    conf.setResponseSpillThreshold(hmsConf.getLong(HMS_RESPONSE_SPILL_THRESHOLD, DEFAULT_HMS_RESPONSE_SPILL_THRESHOLD));
    conf.setHandlerNamePrefix(hmsConf.getString(HMS_HANDLER_NAME_PREFIX, DEFAULT_HMS_HANDLER_NAME_PREFIX));

    return conf;
  }

  /*
   * first load the property file and then override some properties with environment variables
   * since Lambda function could pass in environment variables. In this way, we don't need to
   * recompile the source code for property changes
   *
   * @return HiveMetaStoreConf
   */
  public static HiveMetaStoreConf loadAndOverrideWithEnvironmentVariables()
  {
    HiveMetaStoreConf conf = load();
    // only support HMS_URIS and SPILL_LOCATION for now since most likely we only need to override them
    String hmsUris = System.getenv(ENV_HMS_URIS);
    if (!Strings.isNullOrEmpty(hmsUris)) {
      conf.setMetastoreUri(""); //set to empty string for Hive local client
    }
    String spillLocation = System.getenv(ENV_SPILL_LOCATION);
    if (!Strings.isNullOrEmpty(spillLocation)) {
      conf.setResponseSpillLocation(spillLocation);
    }

    return conf;
  }

  @Override
  public String toString()
  {
    return "{" +
        "connectionURL: " + connectionURL + '\'' +
        ", connectionDriverName: " + connectionDriverName + '\'' +
        ", connectionUserName: " + connectionUserName + '\'' +
        ", connectionPassword: '" + connectionPassword + '\'' +
        ", metaWarehouse: '" + metaWarehouse + '\'' +
        ", metastoreSetUgi: " + metastoreSetUgi +
        ", metastoreUri: '" + metastoreUri + '\'' +
        ", responseSpillThreshold: " + responseSpillThreshold +
        ", responseSpillLocation: '" + responseSpillLocation + '\'' +
        ", handlerNamePrefix: '" + handlerNamePrefix + '\'' +
        '}';
  }
}