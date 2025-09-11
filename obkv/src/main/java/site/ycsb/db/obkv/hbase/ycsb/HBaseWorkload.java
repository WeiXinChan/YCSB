package site.ycsb.db.obkv.hbase.ycsb;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import site.ycsb.*;
import site.ycsb.db.obkv.common.Keys;
import site.ycsb.db.obkv.common.OrderedComparator;
import site.ycsb.db.obkv.common.sql.Column;
import site.ycsb.db.obkv.common.sql.ColumnName;
import site.ycsb.workloads.CoreWorkload;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * workload for obkv hbase
 *
 * @author mokang
 * @date 2025/09/09
 */
public class HBaseWorkload extends CoreWorkload {
  public static final Map<String, Column> ALL_COLUMNS_MAP = new TreeMap<>(new OrderedComparator());
  public static final TreeMap<String, Column> ROW_KEY_COLUMNS_MAP = new TreeMap<>(new OrderedComparator());
  public static final Map<String, Column> DATA_COLUMNS_MAP = new TreeMap<>(new OrderedComparator());
  public static final String JDBC_FORMAT = "jdbc:mysql://%s:%s/%s?useSSL=false&characterEncoding=UTF-8";
  public static final String PARAMETERS_SQL_FORMAT = "SELECT VALUE FROM OCEANBASE.GV$OB_PARAMETERS WHERE NAME = ? LIMIT 1";
  public static final String COLUMN_SQL_FORMAT = "SELECT * FROM OCEANBASE.__ALL_COLUMN WHERE TABLE_ID = (SELECT TABLE_ID FROM OCEANBASE.__ALL_TABLE WHERE TABLE_NAME = ?) ORDER BY COLUMN_ID";
  protected long columnGenLengthLimit;
  protected boolean useNumKey;
  protected boolean supportIncr;

  @Override
  public void init(Properties properties) throws WorkloadException {
    super.init(properties);
    try {
      if (Boolean.parseBoolean(properties.getProperty(Keys.OBKV_PARSE_ARGS.getKey(), Keys.OBKV_PARSE_ARGS.getDefaultValue()))) {
        parseArguments(properties);
      }
      postParse(properties);
    } catch (Exception e) {
      throw new WorkloadException("workload init error", e);
    }
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
        data = buildValueByteIterator(DATA_COLUMNS_MAP.get("V"));
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
      data = buildValueByteIterator(DATA_COLUMNS_MAP.get("V"));
    }
    value.put(fieldkey, data);
    return value;
  }

  protected ByteIterator buildValueByteIterator(Column column) {
    if (supportIncr) {
      return new ByteArrayByteIterator(Bytes.toBytes(ThreadLocalRandom.current().nextLong(1, 10000)));
    }
    long genFiledLength = getFieldLength();
    if (genFiledLength <= 0) {
      long len = Math.min(column.getDataLength(), columnGenLengthLimit);
      return new RandomByteIterator(len);
    } else {
      return new RandomByteIterator(genFiledLength);
    }
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
    String family = properties.getProperty(Keys.OBKV_HBASE_FAMILY.getKey(), Keys.OBKV_HBASE_FAMILY.getDefaultValue());
    if (StringUtils.isBlank(family)) {
      String[] families = properties.getProperty(Keys.OBKV_HBASE_MULTI_FAMILY.getKey(), Keys.OBKV_HBASE_MULTI_FAMILY.getDefaultValue()).split(",");
      family = families[0];
    }
    tableName = tableName + "$" + family;
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
          if ("K".equalsIgnoreCase(column.getColumnName())) {
            ROW_KEY_COLUMNS_MAP.put("K", column);
          }
          if ("V".equalsIgnoreCase(column.getColumnName())) {
            DATA_COLUMNS_MAP.put("V", column);
          }
        }
      }
    }
    if (ALL_COLUMNS_MAP.isEmpty()) {
      throw new WorkloadException("Please make sure the table " + tableName + " exists");
    }
    //must be false
    dataintegrity = false;
    columnGenLengthLimit = Long.parseLong(properties.getProperty(Keys.OBKV_CULUMN_LENGTH_GEN_LIMIT.getKey(), Keys.OBKV_CULUMN_LENGTH_GEN_LIMIT.getDefaultValue()));
    useNumKey = Boolean.parseBoolean(properties.getProperty(Keys.OBKV_WORKLOAD_NUM_KEY.getKey(), Keys.OBKV_WORKLOAD_NUM_KEY.getDefaultValue()));
    supportIncr = Boolean.parseBoolean(properties.getProperty(Keys.OBKV_HBASE_SUPPORT_INCR.getKey(), Keys.OBKV_HBASE_SUPPORT_INCR.getDefaultValue()));
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

  @Override
  public String buildKeyName(long keynum) {
    if (useNumKey) {
      return String.valueOf(keynum);
    }
    return super.buildKeyName(keynum);
  }
}
