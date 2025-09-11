package site.ycsb.db.obkv.table.ycsb;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.property.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.Client;
import site.ycsb.db.obkv.common.Keys;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionHolder {
  private static final Logger logger = LoggerFactory.getLogger("ycsb");
  private static final AtomicInteger CLIENT_INDEX = new AtomicInteger(0);
  private static final List<ObTableClient> OB_TABLE_CLIENT_LIST = new ArrayList<>();
  private static volatile boolean initialized = false;

  private static synchronized void initialize(Properties properties) {
    if (!initialized) {
      try {
        int threadNum = Integer.parseInt(properties.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
        ExecutorService batchExecutorPool = Executors.newFixedThreadPool(Integer.parseInt(properties.getProperty(Keys.OBKV_BATH_EXECUTOR_POOL_SIZE.getKey(), String.valueOf(threadNum))));
        int clientNum = Integer.parseInt(properties.getProperty(Keys.OBKV_TABLE_CLIENT_NUMBERS.getKey(), String.valueOf(threadNum)));
        if (clientNum > 0) {
          for (int i = 0; i < clientNum; i++) {
            ObTableClient client = initTableClient(properties);
            client.setRuntimeBatchExecutor(batchExecutorPool);
            OB_TABLE_CLIENT_LIST.add(client);
          }
        }
        initialized = true;
      } catch (Exception e) {
        logger.error("client init error", e);
      }
    }
  }

  private static ObTableClient initTableClient(Properties properties) {
    ObTableClient tableClient = new ObTableClient();
    boolean odpMode = Boolean.parseBoolean(properties.getProperty(Keys.OBKV_ODP_MODE.getKey(), Keys.OBKV_ODP_MODE.getDefaultValue()));
    if (odpMode) {
      tableClient.setOdpMode(true);
      tableClient.setOdpAddr(properties.getProperty(Keys.OBKV_PROXY_IP.getKey()));
      tableClient.setOdpPort(Integer.parseInt(properties.getProperty(Keys.OBKV_PROXY_RPC_PORT.getKey(), Keys.OBKV_PROXY_RPC_PORT.getDefaultValue())));
      tableClient.setDatabase(properties.getProperty(Keys.OBKV_DATABASE.getKey(), Keys.OBKV_DATABASE.getDefaultValue()));
    } else {
      tableClient.setOdpMode(false);
      tableClient.setParamURL(properties.getProperty(Keys.OBKV_CONFIG_SERVER.getKey()));
      tableClient.setSysUserName(properties.getProperty(Keys.OBKV_SYS_USER.getKey(), Keys.OBKV_SYS_USER.getDefaultValue()));
      tableClient.setSysPassword(properties.getProperty(Keys.OBKV_SYS_PASS.getKey(), Keys.OBKV_SYS_PASS.getDefaultValue()));
    }
    String userName = properties.getProperty(Keys.OBKV_FULL_USERNAME.getKey());
    String password = properties.getProperty(Keys.OBKV_FULL_PASSWORD.getKey(), Keys.OBKV_FULL_PASSWORD.getDefaultValue());
    tableClient.setFullUserName(userName);
    tableClient.setPassword(password);

    tableClient.addProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(), properties.getProperty(Keys.OBKV_CONNECTION_POOL_SIZE.getKey(), Keys.OBKV_CONNECTION_POOL_SIZE.getDefaultValue()));
    try {
      tableClient.init();
    } catch (Exception e) {
      throw new RuntimeException("init obkv client error", e);
    }
    return tableClient;
  }

  public static ObTableClient getTableClient(Properties properties) {
    if (!initialized) {
      initialize(properties);
    }
    return OB_TABLE_CLIENT_LIST.get(CLIENT_INDEX.getAndIncrement() % OB_TABLE_CLIENT_LIST.size());
  }
}
