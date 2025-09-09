package com.pingcap.tikv.util;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*; 
import java.util.*;

/**
 * Unit tests for RowCodecCore and related classes
 */
public class RowCodecHelperTest {

    private RowCodecHelper codec;
    private Map<String, ByteIterator> testData;

    @Before
    public void setUp() {
        codec = new RowCodecHelper(5);
        testData = new HashMap<>();
        testData.put("field0", new StringByteIterator("value0"));
        testData.put("field1", new StringByteIterator("value1"));
        testData.put("field2", new StringByteIterator("value2"));
        testData.put("field3", new StringByteIterator("value3"));
        testData.put("field4", new StringByteIterator("value4"));
    }

    // ========== Constructor Tests ==========

    @Test
    public void testDefaultConstructor() {
        RowCodecHelper defaultCodec = new RowCodecHelper();
        assertNotNull("Default codec should not be null", defaultCodec);

        Map<String, Long> fieldIndices = defaultCodec.getFieldIndices();
        List<String> fields = defaultCodec.getFields();

        assertEquals("Default field count should be 10", 10, fieldIndices.size());
        assertEquals("Default fields list size should be 10", 10, fields.size());

        // Check field names
        for (int i = 0; i < 10; i++) {
            String expectedField = "field" + i;
            assertTrue("Should contain field: " + expectedField, fields.contains(expectedField));
            assertEquals("Field index should match", (Long)(long)i, fieldIndices.get(expectedField));
        }
    }

    @Test
    public void testConstructorWithFieldCount() {
        RowCodecHelper customCodec = new RowCodecHelper(3);
        assertNotNull("Custom codec should not be null", customCodec);

        Map<String, Long> fieldIndices = customCodec.getFieldIndices();
        List<String> fields = customCodec.getFields();

        assertEquals("Field count should be 3", 3, fieldIndices.size());
        assertEquals("Fields list size should be 3", 3, fields.size());

        // Check field names
        for (int i = 0; i < 3; i++) {
            String expectedField = "field" + i;
            assertTrue("Should contain field: " + expectedField, fields.contains(expectedField));
            assertEquals("Field index should match", (Long)(long)i, fieldIndices.get(expectedField));
        }
    }

    @Test
    public void testConstructorWithCustomMapping() {
        Map<String, Long> customIndices = new HashMap<>();
        customIndices.put("custom0", 0L);
        customIndices.put("custom1", 1L);
        customIndices.put("custom2", 2L);

        List<String> customFields = Arrays.asList("custom0", "custom1", "custom2");

        RowCodecHelper customCodec = new RowCodecHelper(customIndices, customFields);
        assertNotNull("Custom codec should not be null", customCodec);

        Map<String, Long> fieldIndices = customCodec.getFieldIndices();
        List<String> fields = customCodec.getFields();

        assertEquals("Field count should be 3", 3, fieldIndices.size());
        assertEquals("Fields list size should be 3", 3, fields.size());

        assertEquals("Should contain custom0", (Long)0L, fieldIndices.get("custom0"));
        assertEquals("Should contain custom1", (Long)1L, fieldIndices.get("custom1"));
        assertEquals("Should contain custom2", (Long)2L, fieldIndices.get("custom2"));
    }

    // ========== Encode/Decode Tests ==========

    @Test
    public void testEncodeDecode() throws Exception {
        // Encode the test data
        byte[] encoded = codec.encode(testData);
        assertNotNull("Encoded data should not be null", encoded);
        assertTrue("Encoded data should not be empty", encoded.length > 0);
        // Decode the data
        Map<String, byte[]> decoded = codec.decode(encoded);

        assertNotNull("Decoded data should not be null", decoded);

        // Verify all fields are present
        assertEquals("Decoded data should have same number of fields", testData.size(), decoded.size());
        for (Map.Entry<String, ByteIterator> entry : testData.entrySet()) {
            String field = entry.getKey();
            entry.getValue().resetOffset();
            byte[] expectedValue = entry.getValue().toArray();
            byte[] actualValue = decoded.get(field);
            assertNotNull("Field " + field + " should not be null", actualValue);
            assertArrayEquals("Field " + field + " values should match", expectedValue, actualValue);
        }
    }

    @Test
    public void testYcsbPutAndRead() throws Exception {
        codec = new RowCodecHelper(1);
        Map<String, ByteIterator> values = new HashMap<>();
        String value = "81. ;~!Es/U5>J584l/-x:=d&\".,=8&7l?A;M9944%asdjkadkaadasdaqeqwess";
        System.out.println("value: " + value.getBytes().length);
        values.put("field0", new StringByteIterator(value));
    
        byte[] encoded = codec.encode(values);
        assertNotNull("Encoded data should not be null", encoded);
        System.out.println("encoded: " + ByteString.copyFrom(encoded).toStringUtf8());
        Map<String, byte[]> decoded = codec.decode(encoded);
        for (Map.Entry<String, byte[]> entry : decoded.entrySet()) {
            String field = entry.getKey();
            byte[] actualValue = entry.getValue();
            System.out.println("decoded: " + "{" + field + ": " + ByteString.copyFrom(actualValue).toStringUtf8()+"}");
        }
    }

    @Test
    public void testEncodeDecodeWithBuffer() throws Exception {
        byte[] buffer = new byte[100];

        // Encode with buffer (buffer is not used in current implementation)
        byte[] encoded = codec.encode(buffer, testData);
        assertNotNull("Encoded data should not be null", encoded);

        // Decode the data
        Map<String, byte[]> decoded = codec.decode(encoded);
        assertNotNull("Decoded data should not be null", decoded);

        // Verify data integrity
        assertEquals("Decoded data should have same number of fields", testData.size(), decoded.size());
        for (Map.Entry<String, ByteIterator> entry : testData.entrySet()) {
            String field = entry.getKey();
            entry.getValue().resetOffset();
            byte[] expectedValue = entry.getValue().toArray();
            byte[] actualValue = decoded.get(field);

            assertArrayEquals("Field " + field + " values should match", expectedValue, actualValue);
        }
    }

    @Test
    public void testDecodeWithSpecificFields() throws Exception {
        // Encode all data
        byte[] encoded = codec.encode(testData);

        // Decode only specific fields
        List<String> specificFields = Arrays.asList("field1", "field3");
        Map<String, byte[]> decoded = codec.decode(encoded, specificFields);

        assertNotNull("Decoded data should not be null", decoded);
        assertEquals("Should only decode specified fields", 2, decoded.size());

        assertTrue("Should contain field1", decoded.containsKey("field1"));
        assertTrue("Should contain field3", decoded.containsKey("field3"));
        assertFalse("Should not contain field0", decoded.containsKey("field0"));
        assertFalse("Should not contain field2", decoded.containsKey("field2"));
        assertFalse("Should not contain field4", decoded.containsKey("field4"));

        assertArrayEquals("field1 value should match", "value1".getBytes(), decoded.get("field1"));
        assertArrayEquals("field3 value should match", "value3".getBytes(), decoded.get("field3"));
    }

    @Test
    public void testDecodeWithEmptyFields() throws Exception {
        // Encode all data
        byte[] encoded = codec.encode(testData);

        // Decode with empty fields list (should decode all fields)
        Map<String, byte[]> decoded = codec.decode(encoded, new ArrayList<>());

        assertNotNull("Decoded data should not be null", decoded);
        assertEquals("Should decode all fields when fields list is empty", testData.size(), decoded.size());
    }

    @Test
    public void testDecodeWithNullFields() throws Exception {
        // Encode all data
        byte[] encoded = codec.encode(testData);

        // Decode with null fields list (should decode all fields)
        Map<String, byte[]> decoded = codec.decode(encoded, null);

        assertNotNull("Decoded data should not be null", decoded);
        assertEquals("Should decode all fields when fields list is null", testData.size(), decoded.size());
    }

    @Test
    public void testEncodeWithEmptyData() throws Exception {
        Map<String, ByteIterator> emptyData = new HashMap<>();
        byte[] encoded = codec.encode(emptyData);

        assertNotNull("Encoded data should not be null", encoded);

        Map<String, byte[]> decoded = codec.decode(encoded);
        assertNotNull("Decoded data should not be null", decoded);
        assertTrue("Decoded data should be empty", decoded.isEmpty());
    }

    @Test
    public void testEncodeWithUnknownFields() throws Exception {
        Map<String, ByteIterator> dataWithUnknownFields = new HashMap<>();
        dataWithUnknownFields.put("field0", new StringByteIterator("value0"));
        dataWithUnknownFields.put("unknown_field", new StringByteIterator("unknown_value"));

        byte[] encoded = codec.encode(dataWithUnknownFields);
        assertNotNull("Encoded data should not be null", encoded);

        Map<String, byte[]> decoded = codec.decode(encoded);
        assertNotNull("Decoded data should not be null", decoded);

        // Should only contain known fields
        assertEquals("Should only contain known fields", 1, decoded.size());
        assertTrue("Should contain field0", decoded.containsKey("field0"));
        assertFalse("Should not contain unknown_field", decoded.containsKey("unknown_field"));
    }

    // ========== FieldUtils Tests ==========

    @Test
    public void testFieldUtilsCreateFieldIndices() {
        Map<String, Long> indices = FieldUtils.createFieldIndices(3);

        assertNotNull("Field indices should not be null", indices);
        assertEquals("Should have 3 fields", 3, indices.size());

        for (int i = 0; i < 3; i++) {
            String field = "field" + i;
            assertTrue("Should contain field: " + field, indices.containsKey(field));
            assertEquals("Field index should match", (Long)(long)i, indices.get(field));
        }
    }

    @Test
    public void testFieldUtilsCreateFieldIndicesDefault() {
        Map<String, Long> indices = FieldUtils.createFieldIndices();

        assertNotNull("Field indices should not be null", indices);
        assertEquals("Should have default number of fields", 10, indices.size());
    }

    @Test
    public void testFieldUtilsAllFields() {
        List<String> fields = FieldUtils.allFields(3);

        assertNotNull("Fields should not be null", fields);
        assertEquals("Should have 3 fields", 3, fields.size());

        for (int i = 0; i < 3; i++) {
            String expectedField = "field" + i;
            assertTrue("Should contain field: " + expectedField, fields.contains(expectedField));
        }
    }

    @Test
    public void testFieldUtilsAllFieldsDefault() {
        List<String> fields = FieldUtils.allFields();

        assertNotNull("Fields should not be null", fields);
        assertEquals("Should have default number of fields", 10, fields.size());
    }

    // ========== FieldPair Tests ==========

    @Test
    public void testFieldPair() {
        String field = "test_field";
        byte[] value = "test_value".getBytes();

        FieldPair pair = new FieldPair(field, value);

        assertEquals("Field should match", field, pair.getField());
        assertArrayEquals("Value should match", value, pair.getValue());
    }

    @Test
    public void testFieldPairWithNullValue() {
        String field = "test_field";
        byte[] value = null;

        FieldPair pair = new FieldPair(field, value);

        assertEquals("Field should match", field, pair.getField());
        assertNull("Value should be null", pair.getValue());
    }

    // ========== FieldPairs Tests ==========

    @Test
    public void testFieldPairsBasicOperations() {
        FieldPairs pairs = new FieldPairs();

        assertTrue("Should be empty initially", pairs.isEmpty());
        assertEquals("Size should be 0", 0, pairs.size());

        FieldPair pair1 = new FieldPair("field1", "value1".getBytes());
        FieldPair pair2 = new FieldPair("field2", "value2".getBytes());

        pairs.add(pair1);
        pairs.add(pair2);

        assertFalse("Should not be empty", pairs.isEmpty());
        assertEquals("Size should be 2", 2, pairs.size());
        assertTrue("Should contain pair1", pairs.contains(pair1));
        assertTrue("Should contain pair2", pairs.contains(pair2));

        assertEquals("Should get pair1 at index 0", pair1, pairs.get(0));
        assertEquals("Should get pair2 at index 1", pair2, pairs.get(1));
    }

    @Test
    public void testFieldPairsSorting() {
        FieldPairs pairs = new FieldPairs();

        // Add pairs in non-sorted order
        pairs.add(new FieldPair("field2", "value2".getBytes()));
        pairs.add(new FieldPair("field0", "value0".getBytes()));
        pairs.add(new FieldPair("field1", "value1".getBytes()));

        // Sort the pairs
        pairs.sort();

        // Verify sorting
        assertEquals("First field should be field0", "field0", pairs.get(0).getField());
        assertEquals("Second field should be field1", "field1", pairs.get(1).getField());
        assertEquals("Third field should be field2", "field2", pairs.get(2).getField());
    }

    @Test
    public void testNewFieldPairs() {
        Map<String, ByteIterator> values = new HashMap<>();
        values.put("field2", new StringByteIterator("value2"));
        values.put("field0", new StringByteIterator("value0"));
        values.put("field1", new StringByteIterator("value1"));

        FieldPairs pairs = FieldPairs.newFieldPairs(values);

        assertNotNull("FieldPairs should not be null", pairs);
        assertEquals("Should have 3 pairs", 3, pairs.size());

        // Verify sorting
        assertEquals("First field should be field0", "field0", pairs.get(0).getField());
        assertEquals("Second field should be field1", "field1", pairs.get(1).getField());
        assertEquals("Third field should be field2", "field2", pairs.get(2).getField());

        // Verify values
        assertArrayEquals("field0 value should match", "value0".getBytes(), pairs.get(0).getValue());
        assertArrayEquals("field1 value should match", "value1".getBytes(), pairs.get(1).getValue());
        assertArrayEquals("field2 value should match", "value2".getBytes(), pairs.get(2).getValue());
    }

    @Test
    public void testNewFieldPairsFromRowCodecCore() {
        FieldPairs pairs = RowCodecHelper.newFieldPairs(testData);

        assertNotNull("FieldPairs should not be null", pairs);
        assertEquals("Should have same number of pairs as test data", testData.size(), pairs.size());

        // Verify all fields are present and sorted
        List<String> sortedFields = new ArrayList<>(testData.keySet());
        Collections.sort(sortedFields);

        for (int i = 0; i < pairs.size(); i++) {
            assertEquals("Field should match sorted order", sortedFields.get(i), pairs.get(i).getField());
            testData.get(sortedFields.get(i)).resetOffset();
            assertArrayEquals("Value should match", testData.get(sortedFields.get(i)).toArray(), pairs.get(i).getValue());
        }
    }

    // ========== Edge Cases and Error Handling ==========

    @Test
    public void testZeroFieldCount() {
        RowCodecHelper zeroCodec = new RowCodecHelper(0);

        Map<String, Long> fieldIndices = zeroCodec.getFieldIndices();
        List<String> fields = zeroCodec.getFields();

        assertTrue("Field indices should be empty", fieldIndices.isEmpty());
        assertTrue("Fields should be empty", fields.isEmpty());
    }

    @Test
    public void testLargeFieldCount() {
        RowCodecHelper largeCodec = new RowCodecHelper(1000);

        Map<String, Long> fieldIndices = largeCodec.getFieldIndices();
        List<String> fields = largeCodec.getFields();

        assertEquals("Should have 1000 fields", 1000, fieldIndices.size());
        assertEquals("Should have 1000 fields", 1000, fields.size());

        // Check some specific fields
        assertEquals("field0 should have index 0", (Long)0L, fieldIndices.get("field0"));
        assertEquals("field999 should have index 999", (Long)999L, fieldIndices.get("field999"));
    }

    @Test
    public void testEmptyStringFields() {
        Map<String, Long> customIndices = new HashMap<>();
        customIndices.put("", 0L);
        customIndices.put("field1", 1L);

        List<String> customFields = Arrays.asList("", "field1");

        RowCodecHelper customCodec = new RowCodecHelper(customIndices, customFields);

        Map<String, ByteIterator> data = new HashMap<>();
        data.put("", new StringByteIterator("empty_field_value"));
        data.put("field1", new StringByteIterator("field1_value"));

        try {
            byte[] encoded = customCodec.encode(data);
            Map<String, byte[]> decoded = customCodec.decode(encoded);

            assertTrue("Should contain empty string field", decoded.containsKey(""));
            assertTrue("Should contain field1", decoded.containsKey("field1"));
            assertArrayEquals("Empty field value should match", "empty_field_value".getBytes(), decoded.get(""));
            assertArrayEquals("field1 value should match", "field1_value".getBytes(), decoded.get("field1"));
        } catch (Exception e) {
            fail("Should handle empty string fields: " + e.getMessage());
        }
    }

    @Test
    public void testSpecialCharactersInValues() {
        Map<String, ByteIterator> specialData = new HashMap<>();
        specialData.put("field0", new StringByteIterator("value with spaces"));
        specialData.put("field1", new StringByteIterator("value\nwith\nnewlines"));
        specialData.put("field2", new StringByteIterator("value\twith\ttabs"));
        specialData.put("field3", new StringByteIterator("value with unicode: 中文"));
        specialData.put("field4", new ByteArrayByteIterator(new byte[]{0, 1, 2, 3, -1, -2, -3})); // Binary data

        try {
            byte[] encoded = codec.encode(specialData);
            Map<String, byte[]> decoded = codec.decode(encoded);

            assertEquals("Should decode all special fields", specialData.size(), decoded.size());

            for (Map.Entry<String, ByteIterator> entry : specialData.entrySet()) {
                String field = entry.getKey();
                entry.getValue().resetOffset();
                byte[] expectedValue = entry.getValue().toArray();
                byte[] actualValue = decoded.get(field);
                assertArrayEquals("Special value for " + field + " should match", expectedValue, actualValue);
            }
        } catch (Exception e) {
            fail("Should handle special characters: " + e.getMessage());
        }
    }

    @Test
    public void testGetFieldIndicesAndFieldsReturnCopies() {
        Map<String, Long> originalIndices = codec.getFieldIndices();
        List<String> originalFields = codec.getFields();

        // Modify the returned collections
        originalIndices.put("new_field", 999L);
        originalFields.add("new_field");

        // Get fresh copies
        Map<String, Long> freshIndices = codec.getFieldIndices();
        List<String> freshFields = codec.getFields();

        // Original codec should not be affected
        assertFalse("Original codec should not contain new_field in indices", freshIndices.containsKey("new_field"));
        assertFalse("Original codec should not contain new_field in fields", freshFields.contains("new_field"));
    }

}