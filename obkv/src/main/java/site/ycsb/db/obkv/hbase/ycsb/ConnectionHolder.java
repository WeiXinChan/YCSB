package site.ycsb.db.obkv.hbase.ycsb;

import com.alipay.oceanbase.rpc.property.Property;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import site.ycsb.db.obkv.common.Keys;

import java.io.IOException;
import java.util.Properties;

import static com.alipay.oceanbase.hbase.constants.OHConstants.*;

public class ConnectionHolder {
  private static final String CLIENT_IMPL_TYPE_OBKV = "obkv";
  private static final String CLIENT_IMPL_TYPE_HBASE = "hbase";
  private static volatile boolean initialized = false;
  private static Connection connection;

  private static void fillConfiguration(Properties properties, Configuration conf) {
    boolean odpMode = Boolean.parseBoolean(properties.getProperty(Keys.OBKV_ODP_MODE.getKey(), Keys.OBKV_ODP_MODE.getDefaultValue()));
    if (odpMode) {
      // ODP mode
      conf.setBoolean(HBASE_OCEANBASE_ODP_MODE, true);
      conf.set(HBASE_OCEANBASE_ODP_ADDR, properties.getProperty(Keys.OBKV_PROXY_IP.getKey()));
      conf.setInt(HBASE_OCEANBASE_ODP_PORT, Integer.parseInt(properties.getProperty(Keys.OBKV_PROXY_RPC_PORT.getKey(), Keys.OBKV_PROXY_RPC_PORT.getDefaultValue())));
      conf.set(HBASE_OCEANBASE_DATABASE, properties.getProperty(Keys.OBKV_DATABASE.getKey(), Keys.OBKV_DATABASE.getDefaultValue()));
    } else {
      // OCP mode
      conf.setBoolean(HBASE_OCEANBASE_ODP_MODE, false);
      conf.set(HBASE_OCEANBASE_PARAM_URL, properties.getProperty(Keys.OBKV_CONFIG_SERVER.getKey()));
      conf.set(HBASE_OCEANBASE_SYS_USER_NAME, properties.getProperty(Keys.OBKV_SYS_USER.getKey(), Keys.OBKV_SYS_USER.getDefaultValue()));
      conf.set(HBASE_OCEANBASE_SYS_PASSWORD, properties.getProperty(Keys.OBKV_SYS_PASS.getKey(), Keys.OBKV_SYS_PASS.getDefaultValue()));
    }
    String userName = properties.getProperty(Keys.OBKV_FULL_USERNAME.getKey());
    String password = properties.getProperty(Keys.OBKV_FULL_PASSWORD.getKey(), Keys.OBKV_FULL_PASSWORD.getDefaultValue());
    conf.set(HBASE_OCEANBASE_FULL_USER_NAME, userName);
    conf.set(HBASE_OCEANBASE_PASSWORD, password);

    conf.set(Property.SERVER_CONNECTION_POOL_SIZE.getKey(), properties.getProperty(Keys.OBKV_CONNECTION_POOL_SIZE.getKey(), Keys.OBKV_CONNECTION_POOL_SIZE.getDefaultValue()));
  }

  private static synchronized void initialize(Properties properties) {
    if (!initialized) {
      try {
        Configuration conf = HBaseConfiguration.create();
        boolean useKvTable = CLIENT_IMPL_TYPE_OBKV.equalsIgnoreCase(properties.getProperty(Keys.OBKV_HBASE_CONNECTION_TYPE.getKey(), Keys.OBKV_HBASE_CONNECTION_TYPE.getDefaultValue()));
        if (useKvTable) {
          conf.set(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL, "com.alipay.oceanbase.hbase.util.OHConnectionImpl");
          fillConfiguration(properties, conf);
        } else {
          conf.set("hbase.master", properties.getProperty("hbase.master", "127.0.0.1:16000"));
          conf.set(HConstants.ZOOKEEPER_QUORUM, properties.getProperty(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1"));
          conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, properties.getProperty(HConstants.ZOOKEEPER_CLIENT_PORT, "2181"));
          conf.set(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, properties.getProperty(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, "256"));
        }
        connection = ConnectionFactory.createConnection(conf);
        initialized = true;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static Connection getHBaseConnection(Properties properties) throws Exception {
    if (!initialized) {
      initialize(properties);
    }
    return connection;
  }
}
