/*-
 * #%L
 * hms-lambda-func
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

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
//import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import javax.security.auth.login.LoginException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

// Local HMS client to communicate with Hive Metastore
public class EmbeddedHiveMetaStoreClient // implements HiveMetaStoreClient
{
  private static final String HIVE_SITE = "hive-site.xml";
  private static final String CORE_SITE = "core-site.xml";
  // private static final String HADOOP_RPC_PROTECTION = "hadoop.rpc.protection";
  // private static final long SOCKET_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(600);

  private HiveMetaStoreClient client;

  //@Override
  /*public String toString()
  {
    return serverURI.toString();
  }*/

  public EmbeddedHiveMetaStoreClient(HiveConf conf)
      throws TException, IOException, InterruptedException, LoginException, URISyntaxException
  {
      Configuration hadoopConf = new Configuration();
      addResourceFromClassPath(hadoopConf, CORE_SITE);
      addResourceFromClassPath(hadoopConf, HIVE_SITE);
      System.out.println("Creating Embedded Hive Client");
      client = new HiveMetaStoreClient(conf);
  }

  private void addResource(Configuration conf, String filePath) throws MalformedURLException
  {
    File f = new File(filePath);
    if (f.exists() && !f.isDirectory()) {
      conf.addResource(f.toURI().toURL());
    }
  }

  private void addResourceFromClassPath(Configuration conf, String fileName) throws MalformedURLException
  {
    InputStream in = getClass().getResourceAsStream("/" + fileName);
    conf.addResource(in);
  }

  public boolean dbExists(String dbName) throws TException
  {
    return getDatabaseNames(dbName).contains(dbName);
  }

  public boolean tableExists(String dbName, String tableName) throws TException
  {
    return getTableNames(dbName, tableName).contains(tableName);
  }

  public Database getDatabase(String dbName) throws TException
  {
    return client.getDatabase(dbName);
  }

  public Set<String> getDatabaseNames(String filter) throws TException
  {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.getAllDatabases());
    }
    return client.getAllDatabases()
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  // fetch all database objects in one call to avoid multiple lambda apis calls
  public List<Database> getDatabases(String filter) throws TException
  {
    List<Database> databases = new ArrayList<>();
    Set<String> databaseNames = getDatabaseNames(filter);
    if (databaseNames != null && !databaseNames.isEmpty()) {
      for (String databaseName : databaseNames) {
        databases.add(getDatabase(databaseName));
      }
    }

    return databases;
  }

  //@Override
  public List<Database> getDatabasesByNames(List<String> dbNames) throws TException
  {
    List<Database> databases = new ArrayList<>();
    if (dbNames != null && !dbNames.isEmpty()) {
      for (String databaseName : dbNames) {
        databases.add(getDatabase(databaseName));
      }
    }

    return databases;
  }

  public Set<String> getTableNames(String dbName, String filter) throws TException
  {
    if (filter == null || filter.isEmpty()) {
      return new HashSet<>(client.getAllTables(dbName));
    }
    return client.getAllTables(dbName)
        .stream()
        .filter(n -> n.matches(filter))
        .collect(Collectors.toSet());
  }

  public List<Table> getTablesByNames(String dbName, List<String> tableNames)
      throws TException
  {
    return client.getTableObjectsByName(dbName, tableNames);
  }

  public boolean createDatabase(String name) throws TException
  {
    return createDatabase(name, null, null, null);
  }

  public boolean createDatabase(String name,
                                String description,
                                String location,
                                Map<String, String> params)
      throws TException
  {
    Database db = new Database(name, description, location, params);
    client.createDatabase(db);
    return true;
  }

  public boolean createDatabase(Database db) throws TException
  {
    client.createDatabase(db);
    return true;
  }

  public boolean dropDatabase(String dbName) throws TException
  {
    client.dropDatabase(dbName, true, true);
    return true;
  }

  public boolean createTable(Table table) throws TException
  {
    client.createTable(table);
    return true;
  }

  public boolean dropTable(String dbName, String tableName) throws TException
  {
    client.dropTable(dbName, tableName, true, true);
    return true;
  }

  public Table getTable(String dbName, String tableName) throws TException
  {
    return client.getTable(dbName, tableName);
  }

  public Partition createPartition(Table table, List<String> values) throws TException
  {
    return client.add_partition(new PartitionBuilder(table).withValues(values).build());
  }

  public Partition addPartition(Partition partition) throws TException
  {
    return client.add_partition(partition);
  }

  public void addPartitions(List<Partition> partitions) throws TException
  {
    client.add_partitions(partitions);
  }

  public Long getCurrentNotificationId() throws TException
  {
    return client.getCurrentNotificationEventId().getEventId();
  }

  public List<String> getPartitionNames(String dbName, String tableName, short maxSize) throws TException
  {
    return client.listPartitionNames(dbName, tableName, maxSize);
  }

  public boolean dropPartition(String dbName, String tableName,
                               List<String> arguments)
      throws TException
  {
    return client.dropPartition(dbName, tableName, arguments, true);
  }

  public List<Partition> getPartitions(String dbName, String tableName, short maxSize) throws TException
  {
    return client.listPartitions(dbName, tableName, maxSize);
  }

  public DropPartitionsResult dropPartitions(String dbName, String tableName,
                                             List<String> partNames) throws TException
  {
    if (partNames == null) {
      return dropPartitions(dbName, tableName, getPartitionNames(dbName, tableName, (short) -1));
    }
    if (partNames.isEmpty()) {
      return null;
    }
    //return client.dropPartitions(new DropPartitionsRequest(dbName,
     //   tableName, RequestPartsSpec.names(partNames)));
    // return client.dropPartitions(dbName, tableName, getPartitionNames(dbName, tableName, (short) -1), true, true);
    return null;
  }

  public List<Partition> getPartitionsByNames(String dbName, String tableName,
                                              List<String> names) throws TException
  {
    if (names == null) {
      return client.getPartitionsByNames(dbName, tableName,
          getPartitionNames(dbName, tableName, (short) -1));
    }
    return client.getPartitionsByNames(dbName, tableName, names);
  }

  public boolean alterTable(String dbName, String tableName, Table newTable)
      throws TException
  {
    client.alter_table(dbName, tableName, newTable);
    return true;
  }

  public void alterPartition(String dbName, String tableName,
                             Partition partition) throws TException
  {
    client.alter_partition(dbName, tableName, partition);
  }

  public void alterPartitions(String dbName, String tableName,
                              List<Partition> partitions) throws TException
  {
    client.alter_partitions(dbName, tableName, partitions);
  }

  public void appendPartition(String dbName, String tableName,
                              List<String> partitionValues) throws TException
  {
    client.appendPartition(dbName, tableName, partitionValues, null);
  }

  static class PartitionBuilder
  {
    private final Table table;
    private List<String> values;
    private String location;
    private Map<String, String> parameters = new HashMap<>();

    private PartitionBuilder()
    {
      table = null;
    }

    PartitionBuilder(Table table)
    {
      this.table = table;
    }

    PartitionBuilder withValues(List<String> values)
    {
      this.values = new ArrayList<>(values);
      return this;
    }

    PartitionBuilder withLocation(String location)
    {
      this.location = location;
      return this;
    }

    PartitionBuilder withParameter(String name, String value)
    {
      parameters.put(name, value);
      return this;
    }

    PartitionBuilder withParameters(Map<String, String> params)
    {
      parameters = params;
      return this;
    }

    Partition build()
    {
      Partition partition = new Partition();
      List<String> partitionNames = table.getPartitionKeys()
          .stream()
          .map(FieldSchema::getName)
          .collect(Collectors.toList());
      if (partitionNames.size() != values.size()) {
        throw new RuntimeException("Partition values do not match table schema");
      }
      List<String> spec = IntStream.range(0, values.size())
          .mapToObj(i -> partitionNames.get(i) + "=" + values.get(i))
          .collect(Collectors.toList());

      partition.setDbName(table.getDbName());
      partition.setTableName(table.getTableName());
      partition.setParameters(parameters);
      partition.setValues(values);
      partition.setSd(table.getSd().deepCopy());
      if (this.location == null) {
        partition.getSd().setLocation(table.getSd().getLocation() + "/" + Joiner.on("/").join(spec));
      }
      else {
        partition.getSd().setLocation(location);
      }
      return partition;
    }
  }
}