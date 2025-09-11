package site.ycsb.db.obkv.table.ycsb;

import com.alipay.oceanbase.rpc.mutation.Row;
import site.ycsb.DBException;

/**
 * @author mokang
 * @version only for origin ycsb table
 */
public class OriginTableDB extends AbstractTableDB {

  private String rowKeyName;

  @Override
  public void init() throws DBException {
    super.init();
    rowKeyName = rowKeyColumns[0];
  }

  @Override
  public Row buildRwoKey(String table, String key) {
    return new Row(rowKeyName, key);
  }
}
