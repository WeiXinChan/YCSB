package com.pingcap.tikv.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * RowCodec encodes and decodes row data similar to TiDB's tablecodec.
 * Row layout: colID1, value1, colID2, value2, ...
 */
public class RowCodec {
    
    private static final byte COMPACT_BYTES_FLAG = 2;
    private static final byte VARINT_FLAG = 8;
    
    /**
     * Encodes row data and column IDs into a byte array.
     * 
     * @param cols Column values as byte arrays
     * @param colIDs Column IDs
     * @return Encoded byte array
     * @throws Exception if cols and colIDs length don't match
     */
    public static byte[] encodeRow(byte[][] cols, long[] colIDs) throws Exception {
        if (cols.length != colIDs.length) {
            throw new Exception(String.format("EncodeRow error: cols and colIDs count not match %d vs %d", 
                cols.length, colIDs.length));
        }
        
        ByteArrayOutputStream valBuf = new ByteArrayOutputStream();
        
        if (cols.length == 0) {
            valBuf.write(0);
            return valBuf.toByteArray();
        }
        
        for (int i = 0; i < cols.length; i++) {
            encodeInt64(valBuf, colIDs[i]);
            encodeBytes(valBuf, cols[i]);
        }
        
        return valBuf.toByteArray();
    }
    
    /**
     * Decodes a byte array into column map.
     * 
     * @param b Encoded byte array
     * @return Map of column ID to column value
     * @throws Exception if decoding fails
     */
    public static Map<Long, byte[]> decodeRow(byte[] b) throws Exception {
        Map<Long, byte[]> row = new HashMap<>();
        
        if (b.length == 0) {
            return row;
        }
        
        if (b.length == 1 && b[0] == 0) {
            return row;
        }
        
        int offset = 0;
        while (offset < b.length) {
            DecodeResult<Long> idResult = decodeInt64(b, offset);
            long rowID = idResult.value;
            offset = idResult.nextOffset;
            DecodeResult<byte[]> valueResult = decodeBytes(b, offset);
            byte[] value = valueResult.value;
            offset = valueResult.nextOffset;
            
            row.put(rowID, value);
        }
        
        return row;
    }
    
    public static void encodeInt64(ByteArrayOutputStream buf, long v) throws IOException {
        buf.write(VARINT_FLAG);
        appendVarint(buf, v);
    }
    
    public static void encodeBytes(ByteArrayOutputStream buf, byte[] v) throws IOException {
        buf.write(COMPACT_BYTES_FLAG);
        appendVarint(buf, v.length);
        buf.write(v);
    }
    
    public static void appendVarint(ByteArrayOutputStream buf, long v) throws IOException {
        // Java equivalent of Go's binary.PutVarint with ZigZag encoding
        // ZigZag encode: (n << 1) ^ (n >> 63) for 64-bit
        long zigzag = (v << 1) ^ (v >> 63);
        while ((zigzag & 0xFFFFFFFFFFFFFF80L) != 0) {
            buf.write((byte)((zigzag & 0x7F) | 0x80));
            zigzag >>>= 7;
        }
        buf.write((byte)(zigzag & 0x7F));
    }
    
    public static DecodeResult<Long> decodeInt64(byte[] b, int offset) throws Exception {
        return decodeVarint(b, offset + 1); // Skip the flag byte
    }
    
    public static DecodeResult<Long> decodeVarint(byte[] b, int offset) throws Exception {
        long result = 0;
        int shift = 0;
        int i = offset;
        
        while (i < b.length) {
            byte byteVal = b[i];
            if (shift >= 64) {
                throw new Exception("value larger than 64 bits");
            }
            
            if ((byteVal & 0x80) == 0) {
                // Last byte - just add the value without sign extension
                result |= ((long)(byteVal & 0x7F)) << shift;
                // ZigZag decode: (n >> 1) ^ (-(n & 1))
                long decoded = (result >>> 1) ^ (-(result & 1));
                return new DecodeResult<>(decoded, i + 1);
            }
            
            result |= ((long)(byteVal & 0x7F)) << shift;
            shift += 7;
            i++;
        }
        
        throw new Exception("insufficient bytes to decode value");
    }
    
    public static DecodeResult<byte[]> decodeBytes(byte[] b, int offset) throws Exception {
        DecodeResult<Long> lengthResult = decodeVarint(b, offset + 1); // Skip the flag byte
        long length = lengthResult.value;
        int nextOffset = lengthResult.nextOffset;
        
        if (b.length - nextOffset < length) {
            throw new Exception(String.format("insufficient bytes to decode value, expected length: %d", length));
        }
        byte[] result = new byte[(int)length];
        System.arraycopy(b, nextOffset, result, 0, (int)length);
        
        return new DecodeResult<>(result, nextOffset + (int)length);
    }
    
    /**
     * Helper class to return both decoded value and next offset
     */
    public static class DecodeResult<T> {
        final T value;
        final int nextOffset;
        
        DecodeResult(T value, int nextOffset) {
            this.value = value;
            this.nextOffset = nextOffset;
        }
    }
    
    /**
     * Utility method equivalent to Go's util.Slice
     */
    public static byte[] getRowKey(String table, String key) {
        return String.format("%s:%s", table, key).getBytes();
    }
}