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

package com.oceanbase.obkv.ycsb;
import com.alipay.oceanbase.hbase.OHTable;
import com.alipay.oceanbase.rpc.property.Property;
import com.yahoo.ycsb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.*;
import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static com.yahoo.ycsb.Status.*;
import static org.apache.commons.lang.StringUtils.isNotBlank;

public class OBHBaseClient extends DB {
    public static final String COLUMN_FAMILY = "hbase.oceanbase.columnFamily";
    public static final String TABLE         = "hbase.oceanbase.table";
    private String             columnFamily;
    private byte[]             columnFamilyBytes;
    private String             table;
    public boolean             debug         = false;
    private OHTable            ohTable;

    /**
     * 初始化，可以从 java 启动参数中传入
     * @throws DBException exception
     */
    public void init() throws DBException {
        Properties props = getProperties();
        columnFamily = props.getProperty(COLUMN_FAMILY);
        table = props.getProperty(TABLE);
        boolean odpMode = false;
        Configuration conf = new Configuration();

        if (!isNotBlank(props.getProperty(COLUMN_FAMILY))) {
            throw new DBException("columnFamily is blank!");
        }
        if (!isNotBlank(props.getProperty(TABLE))) {
            throw new DBException("table is blank!");
        }
        if (!isNotBlank(props.getProperty(HBASE_OCEANBASE_FULL_USER_NAME))) {
            throw new DBException("full user name is blank!");
        }

        if (props.getProperty(HBASE_OCEANBASE_ODP_MODE) != null) {
            odpMode = Boolean.parseBoolean(props.getProperty(HBASE_OCEANBASE_ODP_MODE));
        }
        if (odpMode) {
            conf.setBoolean(HBASE_OCEANBASE_ODP_MODE, true);
            conf.set(HBASE_OCEANBASE_FULL_USER_NAME,
                props.getProperty(HBASE_OCEANBASE_FULL_USER_NAME));
            conf.set(HBASE_OCEANBASE_PASSWORD, props.getProperty(HBASE_OCEANBASE_PASSWORD));
            if (!isNotBlank(props.getProperty(HBASE_OCEANBASE_ODP_ADDR))) {
                throw new DBException("odp addr is blank!");
            }
            conf.set(HBASE_OCEANBASE_ODP_ADDR, props.getProperty(HBASE_OCEANBASE_ODP_ADDR));
            if (!isNotBlank(props.getProperty(HBASE_OCEANBASE_ODP_PORT))) {
                throw new DBException("odp port is blank!");
            }
            conf.setInt(HBASE_OCEANBASE_ODP_PORT,
                Integer.parseInt(props.getProperty(HBASE_OCEANBASE_ODP_PORT)));
            if (!isNotBlank(props.getProperty(HBASE_OCEANBASE_DATABASE))) {
                throw new DBException("database name is blank!");
            }
            conf.set(HBASE_OCEANBASE_DATABASE, props.getProperty(HBASE_OCEANBASE_DATABASE));
        } else {
            if (!isNotBlank(props.getProperty(HBASE_OCEANBASE_PARAM_URL))) {
                throw new DBException("param url is blank!");
            }
            conf.set(HBASE_OCEANBASE_PARAM_URL, props.getProperty(HBASE_OCEANBASE_PARAM_URL));
            if (!isNotBlank(props.getProperty(HBASE_OCEANBASE_SYS_USER_NAME))) {
                throw new DBException("sys name is blank!");
            }
            conf.set(HBASE_OCEANBASE_SYS_USER_NAME,
                props.getProperty(HBASE_OCEANBASE_SYS_USER_NAME));
            conf.set(HBASE_OCEANBASE_SYS_PASSWORD, props.getProperty(HBASE_OCEANBASE_SYS_PASSWORD));
            conf.set(HBASE_OCEANBASE_FULL_USER_NAME,
                props.getProperty(HBASE_OCEANBASE_FULL_USER_NAME));
            conf.set(HBASE_OCEANBASE_PASSWORD, props.getProperty(HBASE_OCEANBASE_PASSWORD));
        }
        // Some other useful property
        for (Property property : Property.values()) {
            String value = props.getProperty(property.getKey());
            if (value != null) {
                conf.set(property.getKey(), value);
            }
        }

        try {
            ohTable = new OHTable(conf, table);
        } catch (Exception e) {
            throw new DBException(e);
        }
        if ((getProperties().getProperty("debug") != null)
            && (getProperties().getProperty("debug").compareTo("true") == 0)) {
            debug = true;
        }
        columnFamilyBytes = Bytes.toBytes(columnFamily);
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
        Result r = null;
        try {
            if (debug) {
                System.out.println("Doing read from HBase columnfamily " + columnFamily);
                System.out.println("Doing read for key: " + key);
            }
            Get g = new Get(Bytes.toBytes(key));
            if (fields == null) {
                g.addFamily(columnFamilyBytes);
            } else {
                for (String field : fields) {
                    g.addColumn(columnFamilyBytes, Bytes.toBytes(field));
                }
            }
            r = ohTable.get(g);
        } catch (IOException e) {
            System.err.println("Error doing get: " + e);
            return SERVICE_UNAVAILABLE;
        } catch (ConcurrentModificationException e) {
            return SERVICE_UNAVAILABLE;
        }
        for (KeyValue kv : r.raw()) {
            result.put(Bytes.toString(kv.getQualifier()), new ByteArrayByteIterator(kv.getValue()));
            if (debug) {
                System.out.println("Result for field: " + Bytes.toString(kv.getQualifier())
                                   + " is: " + Bytes.toString(kv.getValue()));
            }
        }
        return OK;
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

        Scan scan = new Scan(Bytes.toBytes(startkey));
        scan.setCaching(recordcount);
        scan.setMaxVersions(1);
//            scan.setStartRow(startkey.getBytes());
//            scan.setStopRow(startkey.getBytes());
        //add specified fields or else all fields
        if (fields == null) {
            scan.addFamily(columnFamilyBytes);
        } else {
            for (String field : fields) {
                scan.addColumn(columnFamilyBytes, Bytes.toBytes(field));
            }
        }

        ResultScanner scanner = null;
        try {
            scanner = ohTable.getScanner(scan);
            int numResults = 0;

            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                // get row key
                String key = Bytes.toString(rr.getRow());

                if (debug) {
                    System.out.println("Got scan result for key: " + key);
                }

                HashMap<String, ByteIterator> rowResult =
                        new HashMap<String, ByteIterator>();

                while (rr.advance()) {
                    final Cell cell = rr.current();
                    rowResult.put(Bytes.toString(CellUtil.cloneQualifier(cell)),
                            new ByteArrayByteIterator(CellUtil.cloneValue(cell)));
                }

                // add rowResult to result vector
                result.add(rowResult);
                numResults++;

                // PageFilter does not guarantee that the number of results is <=
                // pageSize, so this
                // break is required.
                if (numResults >= recordcount) {// if hit recordcount, bail out
                    break;
                }
            }
        } catch (IOException e) {
            if (debug) {
                System.out.println("Error in getting/parsing scan result: " + e);
            }
            return Status.ERROR;
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return Status.OK;
    }

    /**
     * 更新操作，目前不支持批量接口测试
     * @param table table
     * @param key key
     * @param values values
     * @return ans
     */
    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        if (debug) {
            System.out.println("Setting up put for key: " + key);// NOPMD
        }
        Put p = new Put(Bytes.toBytes(key));
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            if (debug) {
                System.out.println("Adding field/value " + entry.getKey() + "/" + entry.getValue()// NOPMD
                                   + " to put request");// NOPMD
            }
            p.add(columnFamilyBytes, Bytes.toBytes(entry.getKey()), entry.getValue().toArray());
        }
        try {
            ohTable.put(p);
        } catch (IOException e) {
            if (debug) {
                System.err.println("Error doing put: " + e);// NOPMD
            }
            return SERVICE_UNAVAILABLE;
        } catch (ConcurrentModificationException e) {
            //do nothing for now...hope this is rare
            return SERVICE_UNAVAILABLE;
        }
        return OK;
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        return update(table, key, values);
    }

    /**
     * 删除接口，目前不支持批量删除测试
     * @param table table
     * @param key key
     * @return ans
     */
    @Override
    public Status delete(String table, String key) {
        Delete delete = new Delete(Bytes.toBytes(key));
        delete.deleteFamily(columnFamilyBytes);
        try {
            ohTable.delete(delete);
            return OK;
        } catch (IOException e) {
            if (debug) {
                System.err.println("Error doing delete: " + e);// NOPMD
            }
            return ERROR;
        }
    }
}