/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * obkv client binding for YCSB.
 *
 * All YCSB records are mapped to a obkv *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.mutation.Insert;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.mutation.Put;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.property.Property;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;

/**
 * YCSB binding for <a href="http://obkv.io/">obkv</a>.
 *
 * See {@code obkv/README.md} for details.
 */
public class OBKVClient extends DB {
  public static final String FULL_USER_NAME          = "obkv.fullUserName";
  public static final String CONFIG_URL              = "obkv.configUrl";
  public static final String PASSWORD                = "obkv.password";
  public static final String SYS_USER_NAME           = "obkv.sysUserName";
  public static final String SYS_PASSWORD            = "obkv.sysPassword";
  public static final String TABLE_NAME              = "obkv.tableName";
  public static final String OPERATION_TIMEOUT       = "obkv.operationTimeout";
  public static final String USE_PUT                 = "obkv.usePut";
  public static final String USE_INSERT_UP           = "obkv.useInsertUp";
  public static final String ROWKEY_NAME             = "obkv.rowKeyName";

  private ObTableClient client = new ObTableClient();;
  private String tableName;
  private boolean usePut = false;
  private boolean useInsertUp = false;
  private String operationTimeout;
  private String rowKeyName = "ycsb_key";
  
  public void init() throws DBException {
    Properties props = getProperties();
    String fullUserName = props.getProperty(FULL_USER_NAME);
    if (fullUserName == null) {
      throw new DBException("fullUserName is not set");
    }
    client.setFullUserName(fullUserName);

    String configUrl = props.getProperty(CONFIG_URL);
    if (configUrl == null) {
      throw new DBException("configUrl is not set");
    }
    client.setParamURL(configUrl);

    String password = props.getProperty(PASSWORD);
    if (password == null) {
      throw new DBException("password is not set");
    }
    client.setPassword(password);

    String sysUserName = props.getProperty(SYS_USER_NAME);
    if (sysUserName == null) {
      throw new DBException("sysUserName is not set");
    }
    client.setSysUserName(sysUserName);

    String sysPassword = props.getProperty(SYS_PASSWORD);
    if (sysPassword == null) {
      throw new DBException("sysPassword is not set");
    }
    client.setSysPassword(sysPassword);

    tableName = (String) props.getOrDefault(TABLE_NAME, "usertable");

    rowKeyName = (String) props.getOrDefault(ROWKEY_NAME, "ycsb_key");
    usePut = Boolean.parseBoolean(props.getProperty(USE_PUT, "false"));
    useInsertUp = Boolean.parseBoolean(props.getProperty(USE_INSERT_UP, "false"));
    operationTimeout = (String) props.getOrDefault(OPERATION_TIMEOUT, "3000");
    client.addProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), operationTimeout);

    try {
      client.init();
    } catch (Exception e) {
      throw new DBException(e.toString());
    }
  }

  public void cleanup() throws DBException {
    try {
      client.close();
    } catch (IOException e) {
      throw new DBException("closing failed.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return Status.NOT_IMPLEMENTED;
  }

  private Status put(String table, String key, Map<String, ByteIterator> values) {
    try {
      Put put = client.put(tableName);
      put.setRowKey(colVal(rowKeyName, key));

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String propName = entry.getKey();
        ByteIterator propValue = entry.getValue();
        put.addMutateColVal(colVal(propName, propValue.toString()));
      }

      MutationResult res = put.execute();
      if (res.getAffectedRows() != 1) {
        return Status.ERROR;
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return Status.ERROR;
    }

    return Status.OK;
  }

  private Status insertUp(String table, String key, Map<String, ByteIterator> values) {
    try {
      InsertOrUpdate insertUp = client.insertOrUpdate(tableName);
      insertUp.setRowKey(colVal(rowKeyName, key));

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String propName = entry.getKey();
        ByteIterator propValue = entry.getValue();
        insertUp.addMutateColVal(colVal(propName, propValue.toString()));
      }

      MutationResult res = insertUp.execute();
      if (res.getAffectedRows() != 1 || res.getAffectedRows() != 2) {
        return Status.ERROR;
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (usePut) {
      return put(table, key, values);
    }

    if (useInsertUp) {
      return insertUp(table, key, values);
    }

    try {
      Insert insert = client.insert(tableName);
      insert.setRowKey(colVal(rowKeyName, key));

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String propName = entry.getKey();
        ByteIterator propValue = entry.getValue();
        insert.addMutateColVal(colVal(propName, propValue.toString()));
      }

      MutationResult res = insert.execute();
      if (res.getAffectedRows() != 1) {
        return Status.ERROR;
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

}
