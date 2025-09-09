package site.ycsb.db.obkv.table.ycsb;

import site.ycsb.ByteIterator;
import site.ycsb.NumericByteIterator;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * A byte iterator that handles encoding and decoding date/time values.
 * It can represent either a Date or a Timestamp based on the isTimeStamp flag.
 */
public class DateByteIterator extends ByteIterator {
  private final boolean isTimeStamp;
  private final NumericByteIterator secondsByteIterator;

  /**
   * Creates a DateByteIterator representing a Date object.
   *
   * @param seconds The number of seconds since the epoch
   */
  public DateByteIterator(long seconds) {
    this(seconds, false);
  }

  /**
   * Creates a DateByteIterator.
   *
   * @param seconds     The number of seconds since the epoch
   * @param isTimeStamp If true, represents a Timestamp; otherwise represents a Date
   */
  public DateByteIterator(long seconds, boolean isTimeStamp) {
    secondsByteIterator = new NumericByteIterator(seconds);
    this.isTimeStamp = isTimeStamp;
  }


  @Override
  public boolean hasNext() {
    return secondsByteIterator.hasNext();
  }

  @Override
  public byte nextByte() {
    return secondsByteIterator.nextByte();
  }

  @Override
  public long bytesLeft() {
    return secondsByteIterator.bytesLeft();
  }

  @Override
  public Object getObject() {
    long seconds = (Long) secondsByteIterator.getObject();
    if (isTimeStamp) {
      return new Timestamp(seconds * 1000L);
    } else {
      return LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds), ZoneId.systemDefault());
    }
  }
}
