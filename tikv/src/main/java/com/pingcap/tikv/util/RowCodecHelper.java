package com.pingcap.tikv.util;

import com.yahoo.ycsb.ByteIterator;

import java.util.*;

/**
 * RowCodecHelper is a helper class for RowCodec.
 * It supports field name mapping and provides additional functionality.
 */

public class RowCodecHelper {
  private final Map<String, Long> fieldIndices; // field name to index mapping
  private final List<String> fields; // field names

  /**
   * Creates a RowCodecHelper with default field configuration
   */
  public RowCodecHelper() {
      this.fieldIndices = FieldUtils.createFieldIndices();
      this.fields = FieldUtils.allFields();
  }

  /**
   * Creates a RowCodecHelper with specified field count
   *
   * @param fieldCount Number of fields to create
   */
  public RowCodecHelper(long fieldCount) {
      this.fieldIndices = FieldUtils.createFieldIndices(fieldCount);
      this.fields = FieldUtils.allFields(fieldCount);
  }

  /**
   * Creates a RowCodecHelper with custom field configuration
   *
   * @param fieldIndices Field name to index mapping
   * @param fields List of field names
   */
  public RowCodecHelper(Map<String, Long> fieldIndices, List<String> fields) {
      this.fieldIndices = new HashMap<>(fieldIndices);
      this.fields = new ArrayList<>(fields);
  }

  /**
   * Decodes the row and returns a field-value map.
   * This is equivalent to Go's Decode method.
   *
   * @param row Encoded row data
   * @param fields List of fields to decode (if null, decodes all fields)
   * @return Map of field names to values
   * @throws Exception if decoding fails
   */
  public Map<String, byte[]> decode(byte[] row, List<String> fields) throws Exception {
      if (fields == null || fields.isEmpty()) {
          fields = this.fields;
      }

      // Use existing RowCodec.decodeRow method
      Map<Long, byte[]> data = RowCodec.decodeRow(row);

      Map<String, byte[]> result = new HashMap<>(fields.size());
      for (String field : fields) {
          Long index = fieldIndices.get(field);
          if (index != null && data.containsKey(index)) {
              result.put(field, data.get(index));
          }
      }

      return result;
  }

  /**
   * Decodes the row and returns all fields.
   *
   * @param row Encoded row data
   * @return Map of field names to values
   * @throws Exception if decoding fails
   */
  public Map<String, byte[]> decode(byte[] row) throws Exception {
      return decode(row, null);
  }

  /**
   * Encodes the values into row data.
   * This is equivalent to Go's Encode method.
   *
   * @param buf Buffer to append to (not used in this implementation)
   * @param values Map of field names to values
   * @return Encoded row data
   * @throws Exception if encoding fails
   */
  public byte[] encode(byte[] buf, Map<String, ByteIterator> values) throws Exception {
    
      List<byte[]> cols = new ArrayList<>(values.size());
      List<Long> colIDs = new ArrayList<>(values.size());

      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          String field = entry.getKey();
          byte[] value = entry.getValue().toArray();

          Long index = fieldIndices.get(field);
          if (index != null) {
              cols.add(value);
              colIDs.add(index);
          }
      }

      // Convert to arrays for existing RowCodec.encodeRow method
      byte[][] colsArray = cols.toArray(new byte[0][]);
      long[] colIDsArray = colIDs.stream().mapToLong(Long::longValue).toArray();
      // Use existing RowCodec.encodeRow method
      return RowCodec.encodeRow(colsArray, colIDsArray);
  }

  /**
   * Encodes the values into row data.
   *
   * @param values Map of field names to values
   * @return Encoded row data
   * @throws Exception if encoding fails
   */
  public byte[] encode(Map<String, ByteIterator> values) throws Exception {
      return encode(null, values);
  }

  /**
   * Gets the field indices mapping
   *
   * @return Map of field names to indices
   */
  public Map<String, Long> getFieldIndices() {
      return new HashMap<>(fieldIndices);
  }

  /**
   * Gets the fields list
   *
   * @return List of field names
   */
  public List<String> getFields() {
      return new ArrayList<>(fields);
  }

  /**
   * Creates sorted field pairs from a map.
   * This is equivalent to Go's NewFieldPairs function.
   *
   * @param values Map of field names to values
   * @return Sorted FieldPairs
   */
  public static FieldPairs newFieldPairs(Map<String, ByteIterator> values) {
      return FieldPairs.newFieldPairs(values);
  }
}

/**
 * Utility functions for field management.
 * These are equivalent to Go's createFieldIndices and allFields functions.
 */
class FieldUtils {
  private static final long FIELD_COUNT_DEFAULT = 10;

  /**
   * Creates field indices mapping (equivalent to Go's createFieldIndices)
   *
   * @param fieldCount Number of fields to create
   * @return Map of field names to indices
   */
  public static Map<String, Long> createFieldIndices(long fieldCount) {
      Map<String, Long> m = new HashMap<>((int) fieldCount);
      for (long i = 0; i < fieldCount; i++) {
          String field = String.format("field%d", i);
          m.put(field, i);
      }
      return m;
  }

  /**
   * Creates field indices mapping with default field count
   *
   * @return Map of field names to indices
   */
  public static Map<String, Long> createFieldIndices() {
      return createFieldIndices(FIELD_COUNT_DEFAULT);
  }

  /**
   * Creates all fields list (equivalent to Go's allFields)
   *
   * @param fieldCount Number of fields to create
   * @return List of field names
   */
  public static List<String> allFields(long fieldCount) {
      List<String> fields = new ArrayList<>((int) fieldCount);
      for (long i = 0; i < fieldCount; i++) {
          String field = String.format("field%d", i);
          fields.add(field);
      }
      return fields;
  }

  /**
   * Creates all fields list with default field count
   *
   * @return List of field names
   */
  public static List<String> allFields() {
      return allFields(FIELD_COUNT_DEFAULT);
  }
}

/**
 * FieldPair represents a field-value pair for sorting purposes.
 * This is equivalent to Go's FieldPair struct.
 */
class FieldPair {
    private final String field;
    private final byte[] value;

    public FieldPair(String field, byte[] value) {
        this.field = field;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public byte[] getValue() {
        return value;
    }
}

/**
 * FieldPairs is a list of FieldPair that implements sorting functionality.
 * This is equivalent to Go's FieldPairs type.
 */
class FieldPairs implements List<FieldPair> {
    private final List<FieldPair> pairs;

    public FieldPairs() {
        this.pairs = new ArrayList<>();
    }

    public FieldPairs(List<FieldPair> pairs) {
        this.pairs = new ArrayList<>(pairs);
    }

    @Override
    public int size() {
        return pairs.size();
    }

    @Override
    public boolean isEmpty() {
        return pairs.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return pairs.contains(o);
    }

    @Override
    public Iterator<FieldPair> iterator() {
        return pairs.iterator();
    }

    @Override
    public Object[] toArray() {
        return pairs.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return pairs.toArray(a);
    }

    @Override
    public boolean add(FieldPair fieldPair) {
        return pairs.add(fieldPair);
    }

    @Override
    public boolean remove(Object o) {
        return pairs.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return pairs.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends FieldPair> c) {
        return pairs.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends FieldPair> c) {
        return pairs.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return pairs.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return pairs.retainAll(c);
    }

    @Override
    public void clear() {
        pairs.clear();
    }

    @Override
    public FieldPair get(int index) {
        return pairs.get(index);
    }

    @Override
    public FieldPair set(int index, FieldPair element) {
        return pairs.set(index, element);
    }

    @Override
    public void add(int index, FieldPair element) {
        pairs.add(index, element);
    }

    @Override
    public FieldPair remove(int index) {
        return pairs.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return pairs.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return pairs.lastIndexOf(o);
    }

    @Override
    public ListIterator<FieldPair> listIterator() {
        return pairs.listIterator();
    }

    @Override
    public ListIterator<FieldPair> listIterator(int index) {
        return pairs.listIterator(index);
    }

    @Override
    public List<FieldPair> subList(int fromIndex, int toIndex) {
        return pairs.subList(fromIndex, toIndex);
    }

    /**
     * Sorts the field pairs by field name (equivalent to Go's sort.Sort)
     */
    public void sort() {
        pairs.sort(Comparator.comparing(FieldPair::getField));
    }

    /**
     * Creates a new FieldPairs from a map and sorts it by field names.
     * This is equivalent to Go's NewFieldPairs function.
     *
     * @param values Map of field names to values
     * @return Sorted FieldPairs
     */
    public static FieldPairs newFieldPairs(Map<String, ByteIterator> values) {
        FieldPairs pairs = new FieldPairs();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            pairs.add(new FieldPair(entry.getKey(), entry.getValue().toArray()));
        }
        pairs.sort();
        return pairs;
    }
}