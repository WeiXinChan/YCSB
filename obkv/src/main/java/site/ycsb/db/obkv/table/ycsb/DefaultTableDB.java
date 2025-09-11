package site.ycsb.db.obkv.table.ycsb;

import com.alipay.oceanbase.rpc.mutation.Row;


/**
 * OBKV DefaultTableClient <br/>
 * <span color="yellow">this class is not recommended to use, when your table has multi row key columns or is not a string column,
 * you must override buildRwoKeyColumn method </span>
 * <br/>
 * <br/>
 *
 * <span color="yellow">if you are ready to test origin YCSB usertable use {@link OriginTableDB}</span>
 * <p>
 * <br/>
 * <br/>
 * <span color="yellow">make sure param key is numeric string, use {@link site.ycsb.db.obkv.common.Keys OBKV_WORKLOAD_NUM_KEY} true</span>
 * <br/>
 * <pre color="green">
 *   public Object buildRwoKeyColumn(int index, String key) {
 *     switch (index) {
 *       case 0:
 *         return key;
 *       case 1:
 *         return Integer.parseInt(key);
 *        case 2:
 *         return Long.parseLong(key);
 *       default:
 *         throw new RuntimeException("index out of range");
 *     }
 *   }
 * </pre>
 */

public class DefaultTableDB extends AbstractTableDB {
  @Override
  public Row buildRwoKey(String table, String key) {
    Row row = new Row();
    for (int i = 0; i < rowKeyColumnsLength; i++) {
      row.add(rowKeyColumns[i], buildRwoKeyColumn(i, key));
    }
    return row;
  }

  /**
   * override this method to build row key column object
   *
   * @param index
   * @param key
   * @return
   */
  public Object buildRwoKeyColumn(int index, String key) {
    throw new RuntimeException("you must override this method by sub class");
  }
}
