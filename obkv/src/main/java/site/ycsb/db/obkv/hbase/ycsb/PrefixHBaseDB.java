package site.ycsb.db.obkv.hbase.ycsb;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

public class PrefixHBaseDB extends AbstractHBaseDB {

  private static final String KEY_FORMAT = "%s_%s";


  @Override
  protected String buildHBaseKey(String table, String key) {
    return buildHBaseKey(table, key, key);
  }

  protected String buildHBaseKey(String table, String key1, String key2) {
    long num = Long.parseLong(key1);
    long id = num % hbaseKeyCount;
    return String.format(KEY_FORMAT, id, key2);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Scan scan = new Scan();
    try {
      if (reverseScan) {
        scan.setReversed(true);
        scan.withStartRow(buildHBaseKey(table, startkey, "9").getBytes());
        scan.withStopRow(buildHBaseKey(table, startkey, "0").getBytes());
      } else {
        scan.setReversed(false);
        scan.withStartRow(buildHBaseKey(table, startkey, "0").getBytes());
        scan.withStopRow(buildHBaseKey(table, startkey, "9").getBytes());
      }
      if (!isMultiFamily) {
        scan.addFamily(familyByte);
      }
      ResultScanner scanner = hbaseTable.getScanner(scan);
      Iterator<Result> iterator = scanner.iterator();
      if (iterator.hasNext()) {
        Result r = iterator.next();
        r.getRow();
      }
      scanner.close();
    } catch (Exception e) {
      logger.error("scan error K={}", startkey, e);
    }
    return Status.OK;
  }
}
