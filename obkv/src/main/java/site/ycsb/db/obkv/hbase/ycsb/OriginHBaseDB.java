package site.ycsb.db.obkv.hbase.ycsb;

public class OriginHBaseDB extends AbstractHBaseDB {
  @Override
  protected String buildHBaseKey(String table, String key) {
    return key;
  }
}
