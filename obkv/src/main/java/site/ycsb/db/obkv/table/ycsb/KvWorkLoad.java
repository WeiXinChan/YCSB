package site.ycsb.db.obkv.table.ycsb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import site.ycsb.*;
import site.ycsb.db.obkv.common.Keys;
import site.ycsb.db.obkv.common.OrderedComparator;
import site.ycsb.db.obkv.table.sql.ColumnName;
import site.ycsb.db.obkv.table.sql.Column;
import site.ycsb.db.obkv.table.sql.DataType;
import site.ycsb.generator.UniformLongGenerator;
import site.ycsb.workloads.CoreWorkload;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;

import java.util.regex.Pattern;

/**
 * workload for obkv table
 *
 * @author mokang
 * @date 2025/09/09
 */
public class KvWorkLoad extends CoreWorkload {
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  protected static final Set<DataType> HANDLER_DATA_TYPE_SET = new HashSet<>();
  protected static final Comparator<String> ORDERED_COMPARTOR = new OrderedComparator();
  protected static final Map<String, Column> ALL_COLUMNS_MAP = new TreeMap<>(ORDERED_COMPARTOR);
  protected static final Map<String, Column> ROW_KEY_COLUMNS_MAP = new TreeMap<>(ORDERED_COMPARTOR);
  protected static final Map<String, Column> DATA_COLUMNS_MAP = new TreeMap<>(ORDERED_COMPARTOR);
  protected static final Map<String, Column> SPECIAL_COLUMNS_MAP = new TreeMap<>(ORDERED_COMPARTOR);
  protected static final String JDBC_FORMAT = "jdbc:mysql://%s:%s/%s?useSSL=false&characterEncoding=UTF-8";
  protected static final String PARAMETERS_SQL_FORMAT = "SELECT VALUE FROM OCEANBASE.GV$OB_PARAMETERS WHERE NAME = ? LIMIT 1";
  protected static final String COLUMN_SQL_FORMAT = "SELECT * FROM OCEANBASE.__ALL_COLUMN WHERE TABLE_ID = (SELECT TABLE_ID FROM OCEANBASE.__ALL_TABLE WHERE TABLE_NAME = ?) ORDER BY COLUMN_ID";
  protected long columnGenLengthLimit;
  protected long jsonColumnGenLengthLimit;
  protected long jsonValueLength;
  protected long jsonKeyCount;

  @Override
  public void init(Properties properties) throws WorkloadException {
    super.init(properties);
    try {
      if (Boolean.parseBoolean(properties.getProperty(Keys.OBKV_PARSE_ARGS.getKey(), Keys.OBKV_PARSE_ARGS.getDefaultValue()))) {
        parseArguments(properties);
      }
      initHandleDataTypeSet();
      postParse(properties);
    } catch (Exception e) {
      throw new WorkloadException("workload init error", e);
    }
  }

  protected void initHandleDataTypeSet() {
    HANDLER_DATA_TYPE_SET.add(DataType.ObTinyIntType);
    HANDLER_DATA_TYPE_SET.add(DataType.ObSmallIntType);
    HANDLER_DATA_TYPE_SET.add(DataType.ObInt32Type);
    HANDLER_DATA_TYPE_SET.add(DataType.ObIntType);
    HANDLER_DATA_TYPE_SET.add(DataType.ObVarcharType);
    HANDLER_DATA_TYPE_SET.add(DataType.ObDoubleType);

    HANDLER_DATA_TYPE_SET.add(DataType.ObDateTimeType);
    HANDLER_DATA_TYPE_SET.add(DataType.ObTimestampType);
    HANDLER_DATA_TYPE_SET.add(DataType.ObMySQLDateTimeType);

    // text 类型 也可以通过java.lang.String写入
    HANDLER_DATA_TYPE_SET.add(DataType.ObTinyTextType);
    HANDLER_DATA_TYPE_SET.add(DataType.ObTextType);
    HANDLER_DATA_TYPE_SET.add(DataType.ObMediumTextType);
    HANDLER_DATA_TYPE_SET.add(DataType.ObLongTextType);
    HANDLER_DATA_TYPE_SET.add(DataType.ObJsonType);
  }

  @Override
  public HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<>();

    for (String fieldkey : fieldnames) {
      ByteIterator data;
      if (dataintegrity) {
        //do not go here
        data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
      } else {
        data = buildValueByteIterator(DATA_COLUMNS_MAP.get(fieldkey));
      }
      values.put(fieldkey, data);
    }
    return values;
  }

  @Override
  public HashMap<String, ByteIterator> buildSingleValue(String key) {
    HashMap<String, ByteIterator> value = new HashMap<>();

    String fieldkey = fieldnames.get(fieldchooser.nextValue().intValue());
    ByteIterator data;
    if (dataintegrity) {
      //do not go here
      data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
    } else {
      // fill with random data
      data = buildValueByteIterator(DATA_COLUMNS_MAP.get(fieldkey));
    }
    value.put(fieldkey, data);
    return value;
  }

  protected ByteIterator buildValueByteIterator(Column column) {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    DataType dataType = DataType.fromValue(column.getDataType());
    switch (dataType) {
      case ObTinyIntType:
        return new NumericByteIterator((byte) random.nextInt(0, Byte.MAX_VALUE));
      case ObSmallIntType:
        return new NumericByteIterator((short) random.nextInt(0, Short.MAX_VALUE));
      case ObInt32Type:
        return new NumericByteIterator(random.nextInt(0, Integer.MAX_VALUE));
      case ObIntType:
        return new NumericByteIterator(random.nextLong(0, Integer.MAX_VALUE));
      case ObDoubleType:
        return new NumericByteIterator(random.nextDouble(0, 100.0));
      case ObDateTimeType:
      case ObMySQLDateTimeType:
        return buildDateByteIterator(false);
      case ObTimestampType:
        return buildDateByteIterator(true);
      case ObJsonType:
        return buildJsonByteIterator();
      default:
        long genFiledLength = getFieldLength();
        if (genFiledLength <= 0) {
          long len = Math.min(column.getDataLength(), columnGenLengthLimit);
          return new RandomByteIterator(len);
        } else {
          return new RandomByteIterator(genFiledLength);
        }
    }
  }

  protected ByteIterator buildDateByteIterator(boolean isTimestamp) {
    return new DateByteIterator(ZonedDateTime.now().withNano(0).toInstant().getEpochSecond(), isTimestamp);
  }

  protected ByteIterator buildJsonByteIterator() {
    return new StringByteIterator(buildJsonMap(jsonKeyCount, jsonValueLength));
  }

  private void parseArguments(Properties properties) throws Exception {
    try (Connection connection = getConnection(properties)) {
      String database = properties.getProperty(Keys.OBKV_DATABASE.getKey(), Keys.OBKV_DATABASE.getDefaultValue());
      String fullUsername = properties.getProperty(Keys.OBKV_FULL_USERNAME.getKey());
      boolean odpMode = Boolean.parseBoolean(properties.getProperty(Keys.OBKV_ODP_MODE.getKey(), Keys.OBKV_ODP_MODE.getDefaultValue()));
      String configUrl = "";
      if (!odpMode) {
        if (StringUtils.isBlank(properties.getProperty(Keys.OBKV_CONFIG_SERVER.getKey()))) {
          // 查询 configUrl
          try (PreparedStatement statement = connection.prepareStatement(PARAMETERS_SQL_FORMAT)) {
            statement.setString(1, "obconfig_url");
            try (ResultSet resultSet = statement.executeQuery()) {
              if (resultSet.next()) {
                configUrl = resultSet.getString(1);
              }
            }
          }
          if (StringUtils.isNotBlank(configUrl)) {
            properties.setProperty(Keys.OBKV_CONFIG_SERVER.getKey(), configUrl + "&database=" + database);
            System.out.println(Keys.OBKV_CONFIG_SERVER.getKey() + " is " + properties.getProperty(Keys.OBKV_CONFIG_SERVER.getKey()));
          }
        }
      }

      // root@mysql_tenant
      if (!fullUsername.contains("#")) {
        String obCluster = "";

        if (StringUtils.isNotBlank(configUrl)) {
          Pattern pattern = Pattern.compile("ObCluster=([^&]*)");
          Matcher matcher = pattern.matcher(configUrl);
          if (matcher.find()) {
            obCluster = matcher.group(1);
          }
        } else {
          // 查询 cluster 信息
          try (PreparedStatement statement = connection.prepareStatement(PARAMETERS_SQL_FORMAT)) {
            statement.setString(1, "cluster");
            try (ResultSet resultSet = statement.executeQuery()) {
              if (resultSet.next()) {
                obCluster = resultSet.getString(1);
              }
            }
          }
        }
        System.out.println("ob cluster name is : " + obCluster);
        if (StringUtils.isNotBlank(obCluster)) {
          properties.setProperty(Keys.OBKV_FULL_USERNAME.getKey(), fullUsername + "#" + obCluster);
        }
      }
    }
  }

  private void postParse(Properties properties) throws Exception {
    String tableName = properties.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    Map<String, Field> fieldMap = genColumnFiledMap();
    // 使用 try-with-resources 确保资源正确关闭
    try (Connection connection = getConnection(properties);
         PreparedStatement statement = connection.prepareStatement(COLUMN_SQL_FORMAT)) {
      statement.setString(1, tableName);
      try (ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          Column column = new Column();
          for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
            String key = entry.getKey();
            Field field = entry.getValue();
            field.set(column, resultSet.getObject(key));
          }
          ALL_COLUMNS_MAP.put(column.getColumnName(), column);
          if (column.getRowKeyPosition() > 0) {
            ROW_KEY_COLUMNS_MAP.put(column.getColumnName(), column);
          } else {
            //虚拟生成列
            if ((column.getColumnFlags() & (1L)) == 1) {
              SPECIAL_COLUMNS_MAP.put(column.getColumnName(), column);
              continue;
            }

            //自增列
            if (column.getAutoIncrement() == 1) {
              SPECIAL_COLUMNS_MAP.put(column.getColumnName(), column);
              continue;
            }

            if (HANDLER_DATA_TYPE_SET.contains(DataType.fromValue(column.getDataType()))) {
              DATA_COLUMNS_MAP.put(column.getColumnName(), column);
            }
          }
        }
      }
    }

    if (ALL_COLUMNS_MAP.isEmpty()) {
      throw new WorkloadException("Please make sure the table " + tableName + " exists");
    }
    System.err.println("allColumnsMap is " + ALL_COLUMNS_MAP.keySet());
    System.err.println("rowKeyColumnMap is " + ROW_KEY_COLUMNS_MAP.keySet());
    System.err.println("dataColumnMap is " + DATA_COLUMNS_MAP.keySet());
    System.err.println("specialColumnMap is " + SPECIAL_COLUMNS_MAP.keySet());
    fieldnames = new ArrayList<>(DATA_COLUMNS_MAP.keySet());
    fieldcount = fieldnames.size();
    fieldchooser = new UniformLongGenerator(0, fieldcount - 1);
    //must be false
    dataintegrity = false;
    columnGenLengthLimit = Long.parseLong(properties.getProperty(Keys.OBKV_CULUMN_LENGTH_GEN_LIMIT.getKey(), Keys.OBKV_CULUMN_LENGTH_GEN_LIMIT.getDefaultValue()));
    //attention OBKV_CULUMN_LENGTH_GEN_LIMIT is default value
    jsonColumnGenLengthLimit = Long.parseLong(properties.getProperty(Keys.OBKV_CULUMN_JSON_LENGTH_GEN_LIMIT.getKey(), Keys.OBKV_CULUMN_LENGTH_GEN_LIMIT.getDefaultValue()));
    jsonValueLength = Long.parseLong(properties.getProperty(Keys.OBKV_JSON_VALUE_LENGTH.getKey(), Keys.OBKV_JSON_VALUE_LENGTH.getDefaultValue()));
    jsonKeyCount = jsonColumnGenLengthLimit / jsonValueLength + 1;
  }

  private Connection getConnection(Properties properties) throws Exception {
    String proxyIp = properties.getProperty(Keys.OBKV_PROXY_IP.getKey());
    String proxyPort = properties.getProperty(Keys.OBKV_PROXY_SQL_PORT.getKey(), Keys.OBKV_PROXY_SQL_PORT.getDefaultValue());
    String database = properties.getProperty(Keys.OBKV_DATABASE.getKey(), Keys.OBKV_DATABASE.getDefaultValue());
    String url = String.format(JDBC_FORMAT, proxyIp, proxyPort, database);
    String fullUsername = properties.getProperty(Keys.OBKV_FULL_USERNAME.getKey());
    String password = properties.getProperty(Keys.OBKV_FULL_PASSWORD.getKey(), Keys.OBKV_FULL_PASSWORD.getDefaultValue());
    return DriverManager.getConnection(url, fullUsername, password);
  }

  private Map<String, Field> genColumnFiledMap() {
    Map<String, Field> fieldMap = new HashMap<>();
    Field[] fields = Column.class.getDeclaredFields();
    for (Field f : fields) {
      f.setAccessible(true);
      ColumnName annotation = f.getAnnotation(ColumnName.class);
      if (annotation != null) {
        fieldMap.put(annotation.value(), f);
      } else {
        fieldMap.put(f.getName(), f);
      }
    }
    return fieldMap;
  }

  private String buildJsonMap(long keys, long length) {
    String format = "KEY_%04d";
    Map<String, Object> result = new HashMap<>();
    for (long i = 0; i < keys; i++) {
      String key = String.format(format, i);
      result.put(key, new RandomByteIterator(length).toString());
    }
    try {
      return JSON_MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
