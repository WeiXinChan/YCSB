package com.oceanbase.obkv.ycsb;

import com.alipay.oceanbase.hbase.OHTable;
import com.alipay.oceanbase.rpc.property.Property;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;
import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_OCEANBASE_PASSWORD;
import static org.apache.commons.lang.StringUtils.isNotBlank;

public class OBHBaseClient4RKPart extends DB {
  private static final String KEY_FORMAT = "user%012d_%s";
  public static final String COLUMN_FAMILY = "hbase.oceanbase.columnFamily";
  public static final String TABLE         = "hbase.oceanbase.table";
  private String             columnFamily;
  private byte[]             columnFamilyBytes;
  private String             table;
  public boolean             debug         = false;
  private OHTable ohTable;

  private boolean scanOnePart;
  private long rangePartitionMills;
  private long rangeRowsCount;
  private long totalUidCount;
  private long CURRENT_MILLS;
  private long keyStart;
  private int scanRows;
  private boolean isSamePartInBatch;
  private int batchPutSize;


  public void init() throws DBException {
    Properties props = getProperties();
    Configuration conf = new Configuration();
    // init ObTable
    initConnectConfig(props, conf);
    initRunTimeParams(props);
    // Some other useful property
    for (Property property : Property.values()) {
      String value = props.getProperty(property.getKey());
      if (value != null) {
        conf.set(property.getKey(), value);
      }
    }
    // init ObTable
    try {
      ohTable = new OHTable(conf, table);
    } catch (Exception e) {
      throw new DBException(e);
    }
    if ((getProperties().getProperty("debug") != null)
        && (getProperties().getProperty("debug").compareTo("true") == 0)) {
      debug = true;
    }
  }

  private void initConnectConfig(Properties props, Configuration conf) throws DBException
  {
    columnFamily = props.getProperty(COLUMN_FAMILY);
    columnFamilyBytes = Bytes.toBytes(columnFamily);
    table = props.getProperty(TABLE);
    boolean odpMode = false;

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
  }

  private void initRunTimeParams(Properties props) {
    CURRENT_MILLS = Long.parseLong(props.getProperty("current.mills", String.valueOf(System.currentTimeMillis())));

    scanRows = Integer.parseInt(props.getProperty("scan.rows", String.valueOf(0)));

    keyStart = Long.parseLong(props.getProperty("key.start", props.getProperty("insertstart", "0")));

    //row key的个数
    rangeRowsCount = Long.parseLong(props.getProperty("range.rows.count", String.valueOf(Integer.MAX_VALUE)));
    totalUidCount = Integer.parseInt(props.getProperty("total.uid.count", String.valueOf(20000)));
    // range 分区间隔
    rangePartitionMills = Long.parseLong(props.getProperty("table.range.mills", String.valueOf(3600 * 1000)));

    scanOnePart = Boolean.parseBoolean(props.getProperty("table.scan.one.part", "false"));
    isSamePartInBatch = Boolean.parseBoolean(props.getProperty("batchput.issamepart.per.op", "false"));
    batchPutSize = Integer.parseInt(props.getProperty("batchput.size.per.op", "10"));
    printRuntimeParamsJson();
  }

  private void printRuntimeParamsJson() {
    System.out.println("RuntimeParams:{");
    System.out.println("  \"currentMills\": " + CURRENT_MILLS + ",");
    System.out.println("  \"scanRows\": " + scanRows + ",");
    System.out.println("  \"keyStart\": " + keyStart + ",");
    System.out.println("  \"rangeRowsCount\": " + rangeRowsCount + ",");
    System.out.println("  \"totalUidCount\": " + totalUidCount + ",");
    System.out.println("  \"rangePartitionMills\": " + rangePartitionMills + ",");
    System.out.println("  \"scanOnePart\": " + scanOnePart);
    System.out.println("}");
  }

  private long getKeyTimestamp(long num) {
    int mod = (int) ((num - keyStart) / rangeRowsCount);
    return CURRENT_MILLS + mod * rangePartitionMills + (num - keyStart) % rangePartitionMills;
  }
  private long getPartStartTimestamp(long num) {
    int mod = (int) ((num - keyStart) / rangeRowsCount);
    return CURRENT_MILLS + mod * rangePartitionMills;
  }

  private long getPartEndTimestamp(long num) {
    int mod = (int) ((num - keyStart) / rangeRowsCount);
    return CURRENT_MILLS + (mod + 1) * rangePartitionMills - 1;
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     HashMap<String, ByteIterator> result) {
    throw new RuntimeException("delete is not implemented");
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    int num = Integer.parseInt(startkey);
    Scan scan = new Scan();
    scan.addFamily(columnFamilyBytes);
    scan.setLimit(scanRows);
    try {
      long timeRangeStart = 0;
      long timeRangeEnd = 9;
      //只扫描1个range时才处理时间范围
      if (scanOnePart) {
        long timestamp = getPartStartTimestamp(num);
        scan.setTimeRange(timestamp - 1, getPartEndTimestamp(num));
        if (debug) {
          System.out.println("scan time range: " + (timestamp - 1) + " to " + getPartEndTimestamp(num));
        }
      }
      String rsKey = String.format(KEY_FORMAT, num % totalUidCount, timeRangeStart);
      String reKey = String.format(KEY_FORMAT, num % totalUidCount, timeRangeEnd);
      scan.setStartRow(rsKey.getBytes());
      scan.setStopRow(reKey.getBytes());
      scan.setReversed(false);
      ResultScanner scanner = ohTable.getScanner(scan);
      boolean isEmpty = true;
      if (debug) {
        System.out.println("scan start, rsKey=" + rsKey + ", reKey="+ reKey);
      }
      for (Result r : scanner) {
        isEmpty = false;
        byte[] rowKey = r.getRow();
        if (debug) {
          System.out.println("Scan Result is:" + new String(rowKey));
        }
      }
      if (isEmpty) {
        if (debug) {
          System.out.println("scan empty, rsKey=" + rsKey + ", reKey="+ reKey);
        }
        return Status.NOT_FOUND;
      }
      scanner.close();
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return null;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    return null;
  }

  @Override
  public Status delete(String table, String key) {
    throw new RuntimeException("delete is not implemented");
  }

  public Status batchPut(String table, Map<String, Map<String, ByteIterator>> valuesMap) {
    List<Put> puts = new ArrayList<>();
    if (isSamePartInBatch) {
      Map.Entry<String, Map<String, ByteIterator>> entry = valuesMap.entrySet().iterator().next();
      String key = entry.getKey();
      Map<String, ByteIterator> values = entry.getValue();
      Map<String, String> valMaps = StringByteIterator.getStringMap(values);
      int num = Integer.parseInt(key);
      for (int i = 0; i < batchPutSize; i++) {
          long timestamp = getKeyTimestamp(num) + i;
          String rKey = String.format(KEY_FORMAT, num % totalUidCount, timestamp);
          Put put = new Put(rKey.getBytes());
          valMaps.forEach((k, v) -> put.addColumn(columnFamilyBytes, k.getBytes(), timestamp, v.getBytes()));
          puts.add(put);
      }
    } else {
        for (Map.Entry<String, Map<String, ByteIterator>> entry : valuesMap.entrySet()) {
          String key = entry.getKey();
          Map<String, ByteIterator> values = entry.getValue();
          int num = Integer.parseInt(key);
          Map<String, String> valMaps = StringByteIterator.getStringMap(values);
          long timestamp = getKeyTimestamp(num);
          String rKey = String.format(KEY_FORMAT, num % totalUidCount, timestamp);
          Put put = new Put(rKey.getBytes());
          valMaps.forEach((k, v) -> put.addColumn(columnFamilyBytes, k.getBytes(), timestamp, v.getBytes()));
          puts.add(put);
        }
    }
    try {
      ohTable.put(puts);
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  public Status batchRead(String table, Set<String> fields, Map<String, Map<String, ByteIterator>> valuesMap) {
    throw new RuntimeException("batchRead is not implemented");
  }

}
