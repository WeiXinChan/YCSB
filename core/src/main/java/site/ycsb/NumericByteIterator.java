/**
 * Copyright (c) 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb;

/**
 * A byte iterator that handles encoding and decoding numeric values.
 * Currently this iterator can handle 64 bit signed values and double precision
 * floating point values.
 */
public class NumericByteIterator extends ByteIterator {
  private final byte[] payload;
  private final boolean floatingPoint;
  private int off;
  private final NumericType numericType;

  /**
   * Numeric data types supported by this iterator.
   */
  private enum NumericType {
    LONG,   // 64-bit signed integer
    DOUBLE, // 64-bit floating point
    INT,    // 32-bit signed integer
    SHORT,  // 16-bit signed integer
    BYTE,   // 8-bit signed integer
    BYTE_ARRAY // Raw byte array
  }

  public NumericByteIterator(final long value) {
    this.numericType = NumericType.LONG;
    this.floatingPoint = false;
    this.payload = Utils.longToBytes(value);
    this.off = 0;
  }

  public NumericByteIterator(final double value) {
    this.numericType = NumericType.DOUBLE;
    this.floatingPoint = true;
    this.payload = Utils.doubleToBytes(value);
    this.off = 0;
  }

  public NumericByteIterator(final int value) {
    this.numericType = NumericType.INT;
    this.floatingPoint = false;
    // Use only 4 bytes for int instead of 8 to save space
    this.payload = new byte[4];
    this.payload[0] = (byte) (value >>> 24);
    this.payload[1] = (byte) (value >>> 16);
    this.payload[2] = (byte) (value >>> 8);
    this.payload[3] = (byte) (value >>> 0);
    this.off = 0;
  }

  public NumericByteIterator(final short value) {
    this.numericType = NumericType.SHORT;
    this.floatingPoint = false;
    // Use only 2 bytes for short instead of 8 to save space
    this.payload = new byte[2];
    this.payload[0] = (byte) (value >>> 8);
    this.payload[1] = (byte) (value >>> 0);
    this.off = 0;
  }

  public NumericByteIterator(final byte value) {
    this.numericType = NumericType.BYTE;
    this.floatingPoint = false;
    // Use only 1 byte for byte instead of 8 to save space
    this.payload = new byte[]{value};
    this.off = 0;
  }

  public NumericByteIterator(final byte[] value) {
    this.numericType = NumericType.BYTE_ARRAY;
    this.floatingPoint = false;
    // Create a copy to avoid external modifications
    this.payload = new byte[value.length];
    System.arraycopy(value, 0, this.payload, 0, value.length);
    this.off = 0;
  }

  @Override
  public boolean hasNext() {
    return off < payload.length;
  }

  @Override
  public byte nextByte() {
    return payload[off++];
  }

  @Override
  public long bytesLeft() {
    return payload.length - off;
  }

  @Override
  public void reset() {
    off = 0;
  }

  public long getLong() {
    if (floatingPoint) {
      throw new IllegalStateException("Byte iterator is of the type double");
    }

    switch (numericType) {
      case LONG:
        return Utils.bytesToLong(payload);
      case INT:
        return ((payload[0] & 0xFFL) << 24)
            | ((payload[1] & 0xFFL) << 16)
            | ((payload[2] & 0xFFL) << 8)
            | ((payload[3] & 0xFFL) << 0);
      case SHORT:
        return ((payload[0] & 0xFFL) << 8)
            | ((payload[1] & 0xFFL) << 0);
      case BYTE:
        return payload[0];
      default:
        throw new IllegalStateException("Cannot convert " + numericType + " to long");
    }
  }

  public double getDouble() {
    if (!floatingPoint) {
      throw new IllegalStateException("Byte iterator is of the type long");
    }
    return Utils.bytesToDouble(payload);
  }

  public boolean isFloatingPoint() {
    return floatingPoint;
  }

  @Override
  public Object getObject() {
    switch (numericType) {
      case DOUBLE:
        return getDouble();
      case INT:
        return (int) getLong();
      case SHORT:
        return (short) getLong();
      case BYTE:
        return (byte) getLong();
      case BYTE_ARRAY:
        return payload;
      case LONG:
      default:
        return getLong();
    }
  }
}