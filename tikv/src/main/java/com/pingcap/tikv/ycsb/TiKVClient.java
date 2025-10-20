/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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

package com.pingcap.tikv.ycsb;
import java.util.*;

import com.pingcap.tikv.util.RowCodecHelper;
import com.yahoo.ycsb.*;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;

import static com.yahoo.ycsb.Status.*;
import static com.yahoo.ycsb.workloads.CoreWorkload.FIELD_COUNT_PROPERTY;
import static com.yahoo.ycsb.workloads.CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT;

public class TiKVClient extends DB {
    public static final String PROP_KEY_TIKV_ADDR = "tikv.pd.addr";
    public static final String PROP_KEY_DEBUG = "tikv.debug";
    TiSession session = null;
    RawKVClient client = null;
    RowCodecHelper rowCodecHelper = null;
    boolean debug = false;
    int zeropadding = 0;
    @Override
    public void cleanup() throws DBException {
        try {
            if (client != null) {
                client.close();
              }
              if (session != null) {
                session.close();
              }
        } catch (Exception e) {
            throw new DBException(e);
        } finally {
            client = null;
            session = null;
        }
    }

    /**
    * 初始化，可以从 java 启动参数中传入
    * @throws DBException exception
    */
    public void init() throws DBException {
        Properties props = getProperties();
        debug = Boolean.parseBoolean(props.getProperty(PROP_KEY_DEBUG, "false"));
        int fieldCount =  Integer.parseInt(props.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
        rowCodecHelper = new RowCodecHelper(fieldCount);
        String tikvAddr = props.getProperty(PROP_KEY_TIKV_ADDR);
        if (tikvAddr == null) {
          throw new DBException("tikv.addr is not set");
        }
        TiConfiguration config = TiConfiguration.createRawDefault(tikvAddr);
        config.setRawKVBatchWriteTimeoutInMS(20000);
        session = TiSession.create(config);
        client = session.createRawClient();
        zeropadding = Integer.parseInt(getProperties().getProperty("zeropadding", "12"));
    }

    public byte[] getRowKey(String table, String key) {
        return String.format("%s:%s", table, key).getBytes();
    }

    /**
    * 将零填充的字符串转换为long，加上指定值，再转换回零填充字符串
    * @param paddedKey 零填充的字符串，如"00000028500000"
    * @param increment 要加上的值
    * @param paddingLength 填充长度
    * @return 转换后的零填充字符串
    */
    private String incrementPaddedKey(String paddedKey, long increment, int paddingLength) {
        long keyNum = Long.parseLong(paddedKey);
        long newKeyNum = keyNum + increment;
        return String.format("%0" + paddingLength + "d", newKeyNum);
    }

    /**
    * 将零填充的字符串转换为long，加上指定值，再转换回零填充字符串（使用配置的填充长度）
    * @param paddedKey 零填充的字符串，如"00000028500000"
    * @param increment 要加上的值
    * @return 转换后的零填充字符串
    */
    private String incrementPaddedKey(String paddedKey, long increment) {
        return incrementPaddedKey(paddedKey, increment, zeropadding);
    }

    /**
     * 读取数据测试，目前无法测试批量读取
     * @param table table
     * @param key key
     * @param fields fields
     * @param result result
     * @return ans
   */
    @Override
    public Status read(String table, String key, Set<String> fields,
                    HashMap<String, ByteIterator> result) {
        try {
          byte[] rowKey = getRowKey(table, key);
          Optional<ByteString> row = client.get(ByteString.copyFrom(rowKey));
          if (!row.isPresent()) {
            System.out.println("read rowKey: " + ByteString.copyFrom(rowKey).toStringUtf8() + " not found");
            return Status.NOT_FOUND;
          }
          if (debug) {
            System.out.println("read rowKey: " + ByteString.copyFrom(rowKey).toStringUtf8());
            System.out.println("read rowValue: " + row.get().toStringUtf8());
          }
          Map<String, byte[]> decoded = rowCodecHelper.decode(row.get().toByteArray());
          if (decoded == null || decoded.isEmpty()) {
            System.out.println("read rowKey: " + ByteString.copyFrom(rowKey).toStringUtf8() + " decoded is empty");
            return Status.NOT_FOUND;
          }
          for (Map.Entry<String, byte[]> entry : decoded.entrySet()) {
            result.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue()));
          }
          return OK;
        } catch (Exception e) {
          e.printStackTrace();
          return ERROR;
        }
    }

    private Status put(String table, String key, HashMap<String, ByteIterator> values) {
        try {
          byte[] rowKey = getRowKey(table, key);
          byte[] rowValue = rowCodecHelper.encode(values);
          client.put(ByteString.copyFrom(rowKey), ByteString.copyFrom(rowValue));
          if (debug) {
            System.out.println("put: {rowKey: " + ByteString.copyFrom(rowKey).toStringUtf8() + " rowValue: " + ByteString.copyFrom(rowValue).toStringUtf8() + "}");
          }
          return OK;
        } catch (Exception e) {
          e.printStackTrace();
          return ERROR;
        }
      }

    /**
     * @param table table
     * @param startkey startkey
     * @param recordcount recordcount
     * @param fields fields
     * @param result result
     * @return ans
   */
    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                    Vector<HashMap<String, ByteIterator>> result) {
      byte[] rowKey = getRowKey(table, startkey);
      String endKeyStr = incrementPaddedKey(startkey, recordcount);
      byte[] endKey = getRowKey(table, endKeyStr);
      try {
        List<Kvrpcpb.KvPair> res = client.scan(ByteString.copyFrom(rowKey), ByteString.copyFrom(endKey));
        for (Kvrpcpb.KvPair pair :res) {
          String key = pair.getKey().toStringUtf8();
          ByteString value = pair.getValue();
          Map<String, byte[]> decoded = rowCodecHelper.decode(value.toByteArray(), fields == null ? null : new ArrayList<>(fields));
          HashMap<String, ByteIterator> resultMap = new HashMap<>();
          for (Map.Entry<String, byte[]> entry : decoded.entrySet()) {
            resultMap.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue()));
          }
          result.add(resultMap);
        }
        return OK;
      } catch (Exception e) {
        e.printStackTrace();
        return ERROR;
      }
    }

    /**
   * @param table table
   * @param key key
   * @param values values
   * @return ans
   */
    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        return put(table, key, values);
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        return put(table, key, values);
    }

    /**
    * 删除接口，目前不支持批量删除测试
    * @param table table
    * @param key key
    * @return ans
    */
    @Override
    public Status delete(String table, String key) {
        return NOT_IMPLEMENTED;
    }

    public Status batchPut(String table, Map<String, Map<String, ByteIterator>> valuesMap) {
      Map<ByteString, ByteString> batchPutMap = new HashMap<>();
      try {
        for (Map.Entry<String, Map<String, ByteIterator>> entry : valuesMap.entrySet()) {
          byte[] rowKey = getRowKey(table, entry.getKey());
          byte[] rowValue = rowCodecHelper.encode(entry.getValue());
          if (debug) {
            System.out.println("batchPut: {rowKey: " + ByteString.copyFrom(rowKey).toStringUtf8() + " rowValue: " + ByteString.copyFrom(rowValue).toStringUtf8() + "}");
          }
          batchPutMap.put(ByteString.copyFrom(rowKey), ByteString.copyFrom(rowValue));
        }
        long cost = 0;
        if (debug) {
          cost = System.currentTimeMillis();
        }
        client.batchPut(batchPutMap);
        if (debug) {
          System.out.println("batchPut cost: " + (System.currentTimeMillis() - cost) + "ms");
        }
        return OK;
      } catch (Exception e) {
        e.printStackTrace();
        return ERROR;
      }
    }

    public Status batchRead(String table, Set<String> fields, Map<String, Map<String, ByteIterator>> valuesMap) {
        try {
            List getList = new ArrayList<ByteString>();
            for (String key : valuesMap.keySet()) {
                byte[] rowKey = getRowKey(table, key);
                getList.add(ByteString.copyFrom(rowKey));
            }
            List<Kvrpcpb.KvPair> kvPairs= client.batchGet(getList);
            if (kvPairs == null || kvPairs.isEmpty()) {
              return NOT_FOUND;
            }
            for (int i = 0; i < kvPairs.size(); i++) {
              Kvrpcpb.KvPair kvPair = kvPairs.get(i);
              String key = kvPair.getKey().toStringUtf8();
              ByteString value = kvPair.getValue();
              Map<String, byte[]> decoded = rowCodecHelper.decode(value.toByteArray(), fields == null ? null : new ArrayList<>(fields));
              if (decoded == null || decoded.isEmpty()) {
                return NOT_FOUND;
              }
              HashMap<String, ByteIterator> resultMap = new HashMap<>();
              for (Map.Entry<String, byte[]> entry : decoded.entrySet()) {
                resultMap.put(entry.getKey(), new ByteArrayByteIterator(entry.getValue()));
              }
              if (debug) {
                System.out.println("batchRead result: {key: " + key + " result: " + resultMap + "}");
              }
            }
            return OK;
        } catch (Exception e) {
            e.printStackTrace();
            return ERROR;
        }
    }
}