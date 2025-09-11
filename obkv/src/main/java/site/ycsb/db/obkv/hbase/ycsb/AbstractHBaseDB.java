package site.ycsb.db.obkv.hbase.ycsb;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;
import site.ycsb.db.obkv.common.Keys;
import site.ycsb.db.obkv.table.ycsb.KvWorkload;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractHBaseDB extends DB {

  protected static final Logger logger = LoggerFactory.getLogger("ycsb");
  protected static final Batch.Callback<Object[]> CALLBACK = (region, row, result) -> {
    //TODO
  };
  protected static byte[] familyByte;
  protected Map<String, ObHBaseTableOperationType> HBaseTableOperationTypeMap = buildHBaseOperationTypeMaps();
  protected boolean isMultiFamily = false;
  protected boolean supportIncr;
  protected Table hbaseTable;
  protected boolean enableCellTTL;
  protected boolean verbose;
  protected boolean reverseScan;
  protected long cellTTLTime;
  protected long hbaseKeyCount;
  protected ObHBaseTableOperationType updateType;
  private byte[][] familyByteArray;

  private Map<String, ObHBaseTableOperationType> buildHBaseOperationTypeMaps() {
    Map<String, ObHBaseTableOperationType> map = new HashMap<>();
    for (ObHBaseTableOperationType type : ObHBaseTableOperationType.values()) {
      map.put(type.name().toLowerCase(), type);
    }
    //special
    map.put("delete", ObHBaseTableOperationType.DEL);
    map.put("incr", ObHBaseTableOperationType.INCREMENT);
    return map;
  }

  @Override
  public void init() throws DBException {
    super.init();
    try {
      Properties properties = getProperties();
      verbose = Boolean.parseBoolean(properties.getProperty(Keys.OBKV_VERBOSE_ENABLE.getKey(), Keys.OBKV_VERBOSE_ENABLE.getDefaultValue()));
      reverseScan = Boolean.parseBoolean(properties.getProperty(Keys.OBKV_HBASE_REVERSE_SCAN.getKey(), Keys.OBKV_HBASE_REVERSE_SCAN.getDefaultValue()));
      String family = properties.getProperty(Keys.OBKV_HBASE_FAMILY.getKey(), Keys.OBKV_HBASE_FAMILY.getDefaultValue());
      if (StringUtils.isBlank(family)) {
        isMultiFamily = true;
        String[] familyArray = properties.getProperty(Keys.OBKV_HBASE_MULTI_FAMILY.getKey(), Keys.OBKV_HBASE_MULTI_FAMILY.getDefaultValue()).split(",");
        familyByteArray = Arrays.stream(familyArray).map(String::getBytes).toArray(byte[][]::new);
      } else {
        familyByte = family.getBytes();
      }
      supportIncr = Boolean.parseBoolean(properties.getProperty(Keys.OBKV_HBASE_SUPPORT_INCR.getKey(), Keys.OBKV_HBASE_SUPPORT_INCR.getDefaultValue()));
      enableCellTTL = Boolean.parseBoolean(properties.getProperty(Keys.OBKV_HBASE_CELL_TTL_ENABLE.getKey(), Keys.OBKV_HBASE_CELL_TTL_ENABLE.getDefaultValue()));
      cellTTLTime = Long.parseLong(properties.getProperty(Keys.OBKV_HBASE_CELL_TTL_SECONDS.getKey(), Keys.OBKV_HBASE_CELL_TTL_SECONDS.getDefaultValue()));
      updateType = HBaseTableOperationTypeMap.get(properties.getProperty(Keys.OBKV_HBASE_UPDATE_TYPE.getKey(), Keys.OBKV_HBASE_UPDATE_TYPE.getDefaultValue()));
      hbaseKeyCount = Integer.parseInt(properties.getProperty(Keys.OBKV_HBASE_KEY_COUNT.getKey(), Keys.OBKV_HBASE_KEY_COUNT.getDefaultValue()));
      hbaseTable = getRunTable(properties);
    } catch (Exception e) {
      logger.error("client init error", e);
      //prevent the client from being used after initialization failure
      throw new DBException();
    }
  }

  protected Table getRunTable(Properties properties) throws Exception {
    int threadNum = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    ExecutorService batchExecutorPool = Executors.newFixedThreadPool(Integer.parseInt(properties.getProperty(Keys.OBKV_BATH_EXECUTOR_POOL_SIZE.getKey(), String.valueOf(threadNum))));
    String tableName = properties.getProperty(KvWorkload.TABLENAME_PROPERTY, KvWorkload.TABLENAME_PROPERTY_DEFAULT);
    return ConnectionHolder.getHBaseConnection(properties).getTable(TableName.valueOf(tableName), batchExecutorPool);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Get get = new Get(buildHBaseKey(table, key).getBytes());
    if (!isMultiFamily) {
      get.addFamily(familyByte);
    }
    try {
      Result r = hbaseTable.get(get);
      if (verbose && r.isEmpty()) {
        throw new RuntimeException("read empty");
      }
    } catch (Exception e) {
      logger.error("read error K={}", key, e);
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    //TODO
    throw new RuntimeException("not support yet");
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    byte[] keyBytes = buildHBaseKey(table, key).getBytes();
    try {
      switch (updateType) {
        case PUT:
          Put put = new Put(keyBytes);
          if (isMultiFamily) {
            values.forEach((k, v) -> {
              byte[] qualifier = k.getBytes();
              byte[] val = v.toArray();
              for (byte[] family : familyByteArray) {
                put.addColumn(family, qualifier, val);
              }
            });
          } else {
            values.forEach((k, v) -> put.addColumn(familyByte, Bytes.toBytes(k), v.toArray()));
          }
          if (enableCellTTL) {
            put.setTTL(cellTTLTime);
          }
          hbaseTable.put(put);
          break;
        case INCREMENT:
          Increment increment = new Increment(keyBytes);
          if (!isMultiFamily) {
            values.forEach((k, v) -> increment.addColumn(familyByte, Bytes.toBytes(k), Bytes.toLong(v.toArray())));
          }
          if (enableCellTTL) {
            increment.setTTL(cellTTLTime);
          }
          hbaseTable.increment(increment);
          break;
        case APPEND:
          Append append = new Append(keyBytes);
          if (!isMultiFamily) {
            values.forEach((k, v) -> append.addColumn(familyByte, Bytes.toBytes(k), v.toArray()));
          }
          if (enableCellTTL) {
            append.setTTL(cellTTLTime);
          }
          hbaseTable.append(append);
          break;
        case DEL:
          Delete delete = new Delete(keyBytes);
          if (!isMultiFamily) {
            values.forEach((k, v) -> delete.addColumn(familyByte, Bytes.toBytes(k)));
          }
          hbaseTable.delete(delete);
          break;
        default:
          throw new RuntimeException("unknow update type " + updateType);
      }
    } catch (Exception e) {
      logger.error("update error: ", e);
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      Put put = new Put(buildHBaseKey(table, key).getBytes());
      if (isMultiFamily) {
        values.forEach((k, v) -> {
          byte[] qualifier = k.getBytes();
          byte[] val = v.toArray();
          for (byte[] family : familyByteArray) {
            put.addColumn(family, qualifier, val);
          }
        });
      } else {
        values.forEach((k, v) -> put.addColumn(familyByte, k.getBytes(), v.toArray()));
      }
      if (enableCellTTL) {
        put.setTTL(cellTTLTime);
      }
      hbaseTable.put(put);
    } catch (Exception e) {
      logger.error("insert error K={}", key, e);
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }

  @Override
  public Status batchInsert(String table, Map<String, Map<String, ByteIterator>> valuesMap) {
    List<Row> rows = new ArrayList<>();
    valuesMap.forEach((key, values) -> {
      Put put = new Put(key.getBytes());
      values.forEach((k, v) -> {
        byte[] qualifier = k.getBytes();
        byte[] val = v.toArray();
        for (byte[] family : familyByteArray) {
          put.addColumn(family, qualifier, val);
        }
      });
      if (enableCellTTL) {
        put.setTTL(cellTTLTime);
      }
      rows.add(put);
    });
    try {
      hbaseTable.batchCallback(rows, new Object[rows.size()], CALLBACK);
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status batch(String table, Map<String, Map<String, ByteIterator>> valuesMap) {
    List<Row> rows = new ArrayList<>();
    valuesMap.forEach((key, values) -> {
      byte[] keyBytes = buildHBaseKey(table, key).getBytes();
      switch (updateType) {
        case PUT:
          Put put = new Put(keyBytes);
          if (isMultiFamily) {
            values.forEach((k, v) -> {
              byte[] qualifier = k.getBytes();
              byte[] val = v.toArray();
              for (byte[] family : familyByteArray) {
                put.addColumn(family, qualifier, val);
              }
            });
          } else {
            values.forEach((k, v) -> put.addColumn(familyByte, Bytes.toBytes(k), v.toArray()));
          }
          if (enableCellTTL) {
            put.setTTL(cellTTLTime);
          }
          rows.add(put);
          break;
        case DEL:
          Delete delete = new Delete(keyBytes);
          if (!isMultiFamily) {
            values.forEach((k, v) -> delete.addColumn(familyByte, Bytes.toBytes(k)));
          }
          rows.add(delete);
          break;
        default:
          throw new RuntimeException("unknow update type " + updateType);
      }
    });
    try {
      hbaseTable.batchCallback(rows, new Object[rows.size()], CALLBACK);
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status batchRead(String table, Map<String, Set<String>> valuesMap) {
    List<Row> rows = new ArrayList<>();
    valuesMap.forEach((key, values) -> {
      Get get = new Get(buildHBaseKey(table, key).getBytes());
      if (!isMultiFamily) {
        get.addFamily(familyByte);
      }
      rows.add(get);
    });
    try {
      hbaseTable.batchCallback(rows, new Object[rows.size()], CALLBACK);
    } catch (Exception e) {
      return Status.ERROR;
    }
    return Status.OK;
  }


  /**
   * build your hbase K
   *
   * @param table test table
   * @param key   numeric key
   * @return String
   */
  protected abstract String buildHBaseKey(String table, String key);
}
