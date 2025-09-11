package site.ycsb.db.obkv.common;

public enum Keys {
  //for workload
  BACTH_ENABLE("batch.enable", "false", "batch enable"),
  BATCH_COUNT("batch.count", "10", "batch count"),
  OBKV_WORKLOAD_NUM_KEY("obkv.workload.num.key", "false", "obkv workload num key"),
  OBKV_CULUMN_LENGTH_GEN_LIMIT("obkv.column.gen.limit", "4096", "obkv column length gen limit"),
  OBKV_CULUMN_JSON_LENGTH_GEN_LIMIT("obkv.column.json.gen.limit", "", "obkv column json length gen limit"),
  OBKV_JSON_VALUE_LENGTH("obkv.json.value.length", "100", "obkv json value length"),

  //for obkv connection,
  OBKV_PARSE_ARGS("obkv.parse.args", "true", "obkv parse args"),
  OBKV_ODP_MODE("obkv.odp.mode", "false", "obkv odp mode"),
  OBKV_PROXY_IP("obkv.proxy.ip", "", "obkv proxy ip"),
  OBKV_FULL_USERNAME("obkv.full.username", "", "obkv full user name"),
  OBKV_FULL_PASSWORD("obkv.full.password", "", "obkv full user password"),
  OBKV_PROXY_SQL_PORT("obkv.proxy.port", "3306", "obkv proxy rpc port"),
  OBKV_PROXY_RPC_PORT("obkv.proxy.rpc.port", "3307", "obkv proxy rpc port"),
  OBKV_SYS_USER("obkv.sys.user", "root", "obkv sys user name"),
  OBKV_SYS_PASS("obkv.sys.password", "", "obkv sys user password"),
  OBKV_CONFIG_SERVER("obkv.param.url", "", "obkv config server"),
  OBKV_DATABASE("obkv.database", "test", "obkv database"),
  OBKV_CONNECTION_POOL_SIZE("server.connection.pool.size", "1", "obkv connection pool size"),

  //for table client
  OBKV_TABLE_CLIENT_NUMBERS("obkv.table.client.numbers", "", "obkv table client numbers"),
  OBKV_BATH_EXECUTOR_POOL_SIZE("obkv.batch.executor.pool.size", "", "obkv table client numbers"),
  OBKV_TABLE_INSERT_TYPE("obkv.table.insert.type", "insertup", "obkv table insert type"),
  OBKV_TABLE_UPDATE_TYPE("obkv.table.update.type", "insertup", "obkv table update type"),
  OBKV_TABLE_READ_TYPE("obkv.table.read.type", "get", "obkv table read type"),
  OBKV_HEAP_TABLE("obkv.heap.table", "false", "obkv heap table"),
  OBKV_HEAP_TABLE_INDEX("obkv.heap.table.index", "", "obkv heap table index"),
  OBKV_VERBOSE_ENABLE("obkv.verbose.enable", "false", "obkv verbose enable"),

  //for hbase client
  OBKV_HBASE_CONNECTION_TYPE("obkv.hbase.conn.type", "obkv", "obkv hbase connection type"),
  OBKV_HBASE_FAMILY("obkv.hbase.family", "info", "obkv hbase family"),
  OBKV_HBASE_MULTI_FAMILY("obkv.hbase.multi.family", "info0,info1,info2", "obkv hbase multi family"),
  OBKV_HBASE_KEY_COUNT("obkv.hbase.key.count", String.valueOf(Integer.MAX_VALUE), "obkv hbase key count"),
  OBKV_HBASE_CELL_TTL_ENABLE("obkv.hbase.cell.ttl.enable", "false", "obkv hbase cell ttl enable"),
  OBKV_HBASE_CELL_TTL_SECONDS("obkv.hbase.cell.ttl.seconds", "3600", "obkv hbase cell ttl seconds"),
  OBKV_HBASE_UPDATE_TYPE("obkv.hbase.update.type", "put", "obkv hbase update type"),
  OBKV_HBASE_REVERSE_SCAN("obkv.hbase.reverse.scan", "false", "obkv hbase reverse scan"),
  OBKV_HBASE_SUPPORT_INCR("obkv.hbase.support.incr", "false", "obkv hbase support increment");


  private final String key;
  private final String defaultValue;
  private final String description;

  Keys(String key, String defaultValue, String description) {
    this.key = key;
    this.defaultValue = defaultValue;
    this.description = description;
  }

  public String getKey() {
    return key;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public String getDescription() {
    return description;
  }
}
