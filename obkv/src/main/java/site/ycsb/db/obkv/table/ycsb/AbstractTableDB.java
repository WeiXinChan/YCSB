package site.ycsb.db.obkv.table.ycsb;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Mutation;
import com.alipay.oceanbase.rpc.mutation.MutationFactory;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.db.obkv.common.Keys;

import java.util.*;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;

/**
 * @author mokang
 * @date 2025/09/09
 */
public abstract class AbstractTableDB extends DB {
  protected static final Logger logger = LoggerFactory.getLogger("ycsb");
  protected Map<String, ObTableOperationType> tableOperationTypeMap = buildTableOperationTypeMaps();
  protected ObTableClient client;
  protected boolean isHeapTable;
  protected ObTableOperationType insertType;
  protected ObTableOperationType updateType;
  protected ObTableOperationType readType;
  protected boolean verbose;
  protected String[] allFields;
  protected String[] rowKeyColumns;
  protected int rowKeyColumnsLength;

  private static Map<String, ObTableOperationType> buildTableOperationTypeMaps() {
    Map<String, ObTableOperationType> map = new HashMap<>();
    for (ObTableOperationType type : ObTableOperationType.values()) {
      map.put(type.name().toLowerCase(), type);
    }
    //special
    map.put("delete", ObTableOperationType.DEL);
    map.put("incr", ObTableOperationType.INCREMENT);
    map.put("query", ObTableOperationType.SCAN);
    map.put("insertup", ObTableOperationType.INSERT_OR_UPDATE);
    map.put("insertorupdate", ObTableOperationType.INSERT_OR_UPDATE);
    return map;
  }

  @Override
  public void init() throws DBException {
    super.init();
    Properties properties = getProperties();
    verbose = Boolean.parseBoolean(properties.getProperty(Keys.OBKV_VERBOSE_ENABLE.getKey(), Keys.OBKV_VERBOSE_ENABLE.getDefaultValue()));
    isHeapTable = Boolean.parseBoolean(properties.getProperty(Keys.OBKV_HEAP_TABLE.getKey(), Keys.OBKV_HEAP_TABLE.getDefaultValue()));
    if (isHeapTable) {
      throw new RuntimeException("heap table not support");
    }
    String tableName = properties.getProperty(KvWorkload.TABLENAME_PROPERTY, KvWorkload.TABLENAME_PROPERTY_DEFAULT);
    client = ConnectionHolder.getTableClient(properties);
    insertType = tableOperationTypeMap.get(properties.getProperty(Keys.OBKV_TABLE_INSERT_TYPE.getKey(), Keys.OBKV_TABLE_INSERT_TYPE.getDefaultValue()).toLowerCase());
    updateType = tableOperationTypeMap.get(properties.getProperty(Keys.OBKV_TABLE_UPDATE_TYPE.getKey(), Keys.OBKV_TABLE_UPDATE_TYPE.getDefaultValue()).toLowerCase());
    readType = tableOperationTypeMap.get(properties.getProperty(Keys.OBKV_TABLE_READ_TYPE.getKey(), Keys.OBKV_TABLE_READ_TYPE.getDefaultValue()).toLowerCase());
    allFields = KvWorkload.ALL_COLUMNS_MAP.keySet().toArray(new String[0]);
    rowKeyColumns = KvWorkload.ROW_KEY_COLUMNS_MAP.keySet().toArray(new String[0]);
    rowKeyColumnsLength = rowKeyColumns.length;
    client.addRowKeyElement(tableName, rowKeyColumns);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      Row rowKey = buildRwoKey(table, key);
      switch (readType) {
        case GET:
          Map<String, Object> re = client.get(table, rowKey.getValues(), fields.toArray(new String[0]));
          if (verbose && (Objects.isNull(re) || re.isEmpty())) {
            throw new RuntimeException("read empty");
          }
        case SCAN:
          QueryResultSet resultSet = client.query(table).setRowKey(rowKey).select(fields.toArray(new String[0])).execute();
          if (verbose && (Objects.isNull(resultSet) || resultSet.cacheSize() == 0)) {
            throw new RuntimeException("read empty");
          }
        default:
          throw new RuntimeException("unknown read type " + readType);
      }
    } catch (Exception e) {
      logger.error("read execute error use {} type, key is {} ", readType, key, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    //TODO
    throw new RuntimeException("not support yet");
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    Row rowKey = buildRwoKey(table, key);
    Row row = row();
    if (!ObTableOperationType.DEL.equals(updateType)) {
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        row.add(entry.getKey(), entry.getValue().getObject());
      }
    }
    MutationResult mutationResult;
    try {
      switch (updateType) {
        case UPDATE:
          mutationResult = client.update(table).setRowKey(rowKey).addMutateRow(row).execute();
          break;
        case INSERT_OR_UPDATE:
          mutationResult = client.insertOrUpdate(table).setRowKey(rowKey).addMutateRow(row).execute();
          break;
        case PUT:
          mutationResult = client.put(table).setRowKey(rowKey).addMutateRow(row).execute();
          break;
        case REPLACE:
          mutationResult = client.replace(table).setRowKey(rowKey).addMutateRow(row).execute();
          break;
        case DEL:
          mutationResult = client.delete(table).setRowKey(rowKey).execute();
          break;
        default:
          throw new RuntimeException("unknown update type " + updateType);
      }
      if (verbose && (Objects.isNull(mutationResult) || mutationResult.getAffectedRows() == 0)) {
        throw new RuntimeException("update empty");
      }
    } catch (Exception e) {
      logger.error("update execute error use {} type, key is {} ", updateType, key, e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Row rowKey = buildRwoKey(table, key);
    Row row = row();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      row.add(entry.getKey(), entry.getValue().getObject());
    }
    MutationResult mutationResult;
    try {
      switch (insertType) {
        case INSERT:
          mutationResult = client.insert(table).setRowKey(rowKey).addMutateRow(row).execute();
          break;
        case INSERT_OR_UPDATE:
          mutationResult = client.insertOrUpdate(table).setRowKey(rowKey).addMutateRow(row).execute();
          break;
        case PUT:
          mutationResult = client.put(table).setRowKey(rowKey).addMutateRow(row).execute();
          break;
        case REPLACE:
          mutationResult = client.replace(table).setRowKey(rowKey).addMutateRow(row).execute();
          break;
        default:
          throw new RuntimeException("unknown insert type " + insertType);
      }
      if (verbose && (Objects.isNull(mutationResult) || mutationResult.getAffectedRows() == 0)) {
        throw new RuntimeException("insert empty");
      }
    } catch (Exception e) {
      logger.error("insert execute error use {} type, key is {} ", insertType, key, e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      Row rowKey = buildRwoKey(table, key);
      MutationResult mutationResult = client.delete(table).setRowKey(rowKey).execute();
      if (verbose && (Objects.isNull(mutationResult) || mutationResult.getAffectedRows() == 0)) {
        throw new RuntimeException("delete empty");
      }
    } catch (Exception e) {
      logger.error("delete execute error, key is {} ", key, e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status batchInsert(String table, Map<String, Map<String, ByteIterator>> valuesMap) {
    List<Mutation> mutationList = new ArrayList<>();
    for (Map.Entry<String, Map<String, ByteIterator>> entrySet : valuesMap.entrySet()) {
      String key = entrySet.getKey();
      Map<String, ByteIterator> values = entrySet.getValue();
      Row rowKey = buildRwoKey(table, key);
      Row row = row();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        row.add(entry.getKey(), entry.getValue().getObject());
      }
      switch (insertType) {
        case INSERT:
          mutationList.add(MutationFactory.insert().setRowKey(rowKey).addMutateRow(row));
          break;
        case INSERT_OR_UPDATE:
          mutationList.add(MutationFactory.insertOrUpdate().setRowKey(rowKey).addMutateRow(row));
          break;
        case PUT:
          mutationList.add(MutationFactory.put().setRowKey(rowKey).addMutateRow(row));
          break;
        case REPLACE:
          mutationList.add(MutationFactory.replace().setRowKey(rowKey).addMutateRow(row));
          break;
        default:
          throw new RuntimeException("unknown insert type " + insertType);
      }
    }
    try {
      BatchOperationResult batchOperationResult = client.batchOperation(table).addOperation(mutationList).execute();
      if (verbose && (Objects.isNull(batchOperationResult) || batchOperationResult.hasError())) {
        throw new RuntimeException("batch insert result has error");
      }
    } catch (Exception e) {
      logger.error("batch insert execute error use {} type", insertType, e);
      if (verbose) {
        mutationList.forEach(m -> logger.warn(" batch commit error row is {}", Arrays.toString(m.getRowKey().getValues())));
      }
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status batch(String table, Map<String, Map<String, ByteIterator>> valuesMap) {
    List<Mutation> mutationList = new ArrayList<>();
    for (Map.Entry<String, Map<String, ByteIterator>> entrySet : valuesMap.entrySet()) {
      String key = entrySet.getKey();
      Map<String, ByteIterator> values = entrySet.getValue();
      Row rowKey = buildRwoKey(table, key);
      Row row = row();
      if (!ObTableOperationType.DEL.equals(updateType)) {
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          row.add(entry.getKey(), entry.getValue().getObject());
        }
      }
      switch (updateType) {
        case UPDATE:
          mutationList.add(MutationFactory.update().setRowKey(rowKey).addMutateRow(row));
          break;
        case INSERT_OR_UPDATE:
          mutationList.add(MutationFactory.insertOrUpdate().setRowKey(rowKey).addMutateRow(row));
          break;
        case PUT:
          mutationList.add(MutationFactory.put().setRowKey(rowKey).addMutateRow(row));
          break;
        case REPLACE:
          mutationList.add(MutationFactory.replace().setRowKey(rowKey).addMutateRow(row));
          break;
        case DEL:
          mutationList.add(MutationFactory.delete().setRowKey(rowKey));
          break;
        default:
          throw new RuntimeException("unknown update type " + updateType);
      }
    }
    try {
      BatchOperationResult batchOperationResult = client.batchOperation(table).addOperation(mutationList).execute();
      if (verbose && (Objects.isNull(batchOperationResult) || batchOperationResult.hasError())) {
        throw new RuntimeException("batch result has error");
      }
    } catch (Exception e) {
      logger.error("batch execute error use {} type", updateType, e);
      if (verbose) {
        mutationList.forEach(m -> logger.warn(" batch commit error row is {}", Arrays.toString(m.getRowKey().getValues())));
      }
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status batchRead(String table, Map<String, Set<String>> valuesMap) {
    try {
      BatchOperation batchOperation = client.batchOperation(table);
      TableBatchOps batchOps = client.batch(table);
      for (Map.Entry<String, Set<String>> entrySet : valuesMap.entrySet()) {
        String key = entrySet.getKey();
        String[] selectColumns = entrySet.getValue().toArray(new String[0]);
        Row rowKey = buildRwoKey(table, key);
        switch (readType) {
          case GET:
            batchOps.get(rowKey.getValues(), selectColumns);
            break;
          case SCAN:
            batchOperation.addOperation(MutationFactory.query().setRowKey(rowKey).select(selectColumns));
            break;
          default:
            throw new RuntimeException("unknown read type " + readType);
        }
      }
      if (ObTableOperationType.GET.equals(readType)) {
        List<Object> resultList = batchOps.execute();
        if (verbose && (new BatchOperationResult(resultList).hasError())) {
          throw new RuntimeException("get result has error");
        }
      } else {
        BatchOperationResult batchOperationResult = batchOperation.execute();
        if (verbose && (Objects.isNull(batchOperationResult) || batchOperationResult.hasError())) {
          throw new RuntimeException("get result has error");
        }
      }
    } catch (Exception e) {
      logger.error("read execute error use {} type, key is {} ", readType, e);
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
   * build row key columns object with numeric key  <br/>
   *
   * @param table test table
   * @param key   gen key
   * @return Row
   * @see DefaultTableDB
   */
  public abstract Row buildRwoKey(String table, String key);
}
