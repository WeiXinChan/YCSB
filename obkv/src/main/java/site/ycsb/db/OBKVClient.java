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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.get.Get;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
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
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;

/**
 * YCSB binding for <a href="http://obkv.io/">obkv</a>.
 *
 * See {@code obkv/README.md} for details.
 */
public class OBKVClient extends DB {
  public static final String PROP_KEY_ODP_MODE                = "obkv.isOdpMode";
  public static final String PROP_KEY_ODP_ADDR                = "obkv.odpAddr";
  public static final String PROP_KEY_ODP_PORT                = "obkv.odpPort";
  public static final String PROP_KEY_DATABASE                = "obkv.database";

  public static final String PROP_KEY_FULL_USER_NAME          = "obkv.fullUserName";
  public static final String PROP_KEY_CONFIG_URL              = "obkv.configUrl";
  public static final String PROP_KEY_PASSWORD                = "obkv.password";
  public static final String PROP_KEY_SYS_USER_NAME           = "obkv.sysUserName";
  public static final String PROP_KEY_SYS_PASSWORD            = "obkv.sysPassword";

  public static final String PROP_KEY_TABLE_NAME              = "obkv.tableName";
  public static final String PROP_KEY_BATCH_SIZE              = "obkv.batchSize";
  public static final String PROP_KEY_IS_HEAP_TABLE           = "obkv.isHeapTable";
  public static final String PROP_KEY_DEBUG                   = "obkv.debug";
  public static final String PROP_KEY_DATA_LENGTH             = "obkv.dataLength";
  public static final String PROP_KEY_THREAD_COUNT            = "obkv.threadCount";

  private ObTableClient client = new ObTableClient();
  private String tableName;
  private boolean debug = false;
  private int batchSize = 0;
  private boolean isHeapTable = false;
  private int dataLength = 10240;
  private int threadCount = 3;
  private ExecutorService executorService;

  public void init() throws DBException {
    boolean isOdpMode = false;
    Properties props = getProperties();
    if (props.getProperty(PROP_KEY_ODP_MODE) != null) {
      isOdpMode = Boolean.parseBoolean(props.getProperty(PROP_KEY_ODP_MODE));
      client.setOdpMode(isOdpMode);
    }
    if (isOdpMode) { 
      client.setFullUserName(props.getProperty(PROP_KEY_FULL_USER_NAME));
      client.setOdpAddr(props.getProperty(PROP_KEY_ODP_ADDR));
      client.setOdpPort(Integer.parseInt(props.getProperty(PROP_KEY_ODP_PORT)));
      client.setDatabase(props.getProperty(PROP_KEY_DATABASE));
      client.setPassword(props.getProperty(PROP_KEY_PASSWORD));
    } else {
      client.setFullUserName(props.getProperty(PROP_KEY_FULL_USER_NAME));
      client.setParamURL(props.getProperty(PROP_KEY_CONFIG_URL));
      client.setPassword(props.getProperty(PROP_KEY_PASSWORD));
      client.setSysUserName(props.getProperty(PROP_KEY_SYS_USER_NAME));
      client.setSysPassword(props.getProperty(PROP_KEY_SYS_PASSWORD));
    }

    // Some other useful property
    for (Property property : Property.values()) {
      String value = props.getProperty(property.getKey());
      if (value != null) {
        client.addProperty(property.getKey(), value);
      }
    }

    // batch size
    if (props.getProperty(PROP_KEY_BATCH_SIZE) != null) {
      batchSize = Integer.parseInt(props.getProperty(PROP_KEY_BATCH_SIZE));
    }

    // is heap table
    if (props.getProperty(PROP_KEY_IS_HEAP_TABLE) != null) {
      isHeapTable = Boolean.parseBoolean(props.getProperty(PROP_KEY_IS_HEAP_TABLE));
    }

    // debug
    if (props.getProperty(PROP_KEY_DEBUG) != null) {
      debug = Boolean.parseBoolean(props.getProperty(PROP_KEY_DEBUG));
    }

    if (debug) {
      System.out.println("isOdpMode: " + isOdpMode);
      System.out.println("isHeapTable: " + isHeapTable);
      System.out.println("batchSize: " + batchSize);
    }

    // data length
    if (props.getProperty(PROP_KEY_DATA_LENGTH) != null) {
      dataLength = Integer.parseInt(props.getProperty(PROP_KEY_DATA_LENGTH));
    }

    // thread count
    if (props.getProperty(PROP_KEY_THREAD_COUNT) != null) {
      threadCount = Integer.parseInt(props.getProperty(PROP_KEY_THREAD_COUNT));
    }

    executorService = Executors.newFixedThreadPool(threadCount);
    client.setRuntimeBatchExecutor(executorService);

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
  
  // 高效生成指定长度的随机字符串
  private String generateRandomString(int length) {
    if (length <= 0) {
      return "";
    }
    
    // 使用char数组而不是StringBuilder，减少内存分配
    char[] chars = new char[length];
    
    // 预定义字符集，避免重复创建
    final char[] charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
    final int charSetLength = charSet.length;
    
    // 使用ThreadLocalRandom提高性能，避免synchronized
    java.util.concurrent.ThreadLocalRandom random = java.util.concurrent.ThreadLocalRandom.current();
    
    // 批量生成随机字符，减少方法调用开销
    for (int i = 0; i < length; i++) {
      chars[i] = charSet[random.nextInt(charSetLength)];
    }
    
    return new String(chars);
  }

  // key + i
  private String getPageRowKey(String key, int i) {
    return key + "_" + i;
  }

  // 随机 dataLength/2 长度字符串
  private String getRequest() {
    return generateRandomString(dataLength/2);
  }

  // 随机 dataLength/2 长度字符串
  private String getRawPage() {
    return generateRandomString(dataLength/2);
  }

  // "page" + 随机数
  private String getPageCode() {
    return "page" + (int)(Math.random() * 1000000);
  }

  // 当前时间戳
  private long getTimestamp() {
    return System.currentTimeMillis();
  }

  /*
    CREATE TABLE `usertable` (
      `pagerowkey` varchar(1024) NOT NULL,
      `request` longtext DEFAULT NULL,
      `rawpage` longblob NOT NULL,
      `pagecode` varchar(1024) NOT NULL,
      `timestamp` bigint(20) NOT NULL,
      `_expire_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY(pagerowkey)
    ) TTL(_expire_ts + INTERVAL 30 DAY) PARTITION BY KEY(pagerowkey) PARTITIONS 66;

     CREATE TABLE `usertable` (
      `pagerowkey` varchar(1024) NOT NULL,
      `request` longtext DEFAULT NULL,
      `rawpage` longblob NOT NULL,
      `pagecode` varchar(1024) NOT NULL,
      `timestamp` bigint(20) NOT NULL,
      `_expire_ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE INDEX idx_unique_pagerowkey (pagerowkey)
    ) ORGANIZATION = HEAP TTL(_expire_ts + INTERVAL 30 DAY) PARTITION BY KEY(pagerowkey) PARTITIONS 66;
    
    select length(pagerowkey) + length(request) + length(rawpage) + length(pagecode) from usertable limit 1;
  */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      BatchOperation batch = client.batchOperation(table);
      for (int i = 0; i < batchSize; i++) {
        InsertOrUpdate insUp = client.insertOrUpdate(table);
        insUp.setRowKey(row(colVal("pagerowkey", getPageRowKey(key, i))));
        if (isHeapTable) {
          insUp.addMutateColVal(colVal("pagerowkey", getPageRowKey(key, i)), 
              colVal("request", getRequest()), 
              colVal("rawpage", getRawPage()), 
              colVal("pagecode", getPageCode()), 
              colVal("timestamp", getTimestamp()));
        } else {
          insUp.addMutateColVal(colVal("request", getRequest()), 
              colVal("rawpage", getRawPage()), 
              colVal("pagecode", getPageCode()), 
              colVal("timestamp", getTimestamp()));
        }
        batch.addOperation(insUp);
      }
      batch.execute();
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      BatchOperation batch = client.batchOperation(table);
      for (int i = 0; i < batchSize; i++) {
        Get get = client.get(table);
        get.setRowKey(row(colVal("pagerowkey", getPageRowKey(key, i))));
        batch.addOperation(get);
      }
      batch.execute();
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
