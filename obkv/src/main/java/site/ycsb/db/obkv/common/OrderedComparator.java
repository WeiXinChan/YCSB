package site.ycsb.db.obkv.common;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class OrderedComparator implements Comparator<String> {
  private final ConcurrentHashMap<String, Long> insertionOrder = new ConcurrentHashMap<>();
  private final AtomicLong counter = new AtomicLong(0);

  @Override
  public int compare(String o1, String o2) {
    if (o1 == null && o2 == null) {
      return 0;
    }
    long order1 = insertionOrder.computeIfAbsent(o1, k -> counter.incrementAndGet());
    long order2 = insertionOrder.computeIfAbsent(o2, k -> counter.incrementAndGet());
    // 比较插入顺序，确保后插入的排在后面
    return Long.compare(order1, order2);
  }
}
