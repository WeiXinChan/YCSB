package site.ycsb.db.obkv.common;

public enum Keys {
  OBKV_PARSE_ARGS("obkv.parse.args", "false", "obkv parse args"),
  OBKV_ODP_MODE("obkv.odp.mode", "false", "obkv odp mode"),
  OBKV_PROXY_IP("obkv.proxy.ip", "", "obkv proxy ip"),
  OBKV_FULL_USERNAME("obkv.full.username", "", "obkv full user name"),
  OBKV_FULL_PASSWORD("obkv.full.password", "", "obkv full user password"),
  OBKV_PROXY_SQL_PORT("obkv.proxy.port", "3306", "obkv proxy rpc port"),
  OBKV_PROXY_RPC_PORT("obkv.proxy.rpc.port", "3307", "obkv proxy rpc port"),
  OBKV_SYS_USER("obkv.sys.user", "root", "obkv sys user name"),
  OBKV_SYS_PASS("obkv.sys.password", "", "obkv sys user password"),
  OBKV_CONFIG_SERVER("obkv.param.url", "", "obkv config server"),
  OBKV_CULUMN_LENGTH_GEN_LIMIT("obkv.column.gen.limit", "4096", "obkv column length gen limit"),
  OBKV_CULUMN_JSON_LENGTH_GEN_LIMIT("obkv.column.json.gen.limit", "", "obkv column json length gen limit"),
  OBKV_JSON_VALUE_LENGTH("obkv.json.value.length", "100", "obkv json value length"),
  OBKV_DATABASE("obkv.database", "test", "obkv database");


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
