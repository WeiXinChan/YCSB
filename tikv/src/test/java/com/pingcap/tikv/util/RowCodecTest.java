// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package com.pingcap.tikv.util;

import org.junit.Test;
import org.tikv.shade.com.google.protobuf.ByteString;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.util.Map;

/**
 * Unit tests for RowCodec
 */
public class RowCodecTest {

    @Test
    public void testCodec() throws Exception {
        long[] colIDs = {1, 4, 7, 2, 5, 8};
        byte[][] cols = {
            "147".getBytes(),
            "258".getBytes(), 
            "147258".getBytes(),
            "".getBytes(),
            "258147".getBytes(),
            "369".getBytes()
        };

        // Encode the row
        byte[] buf = RowCodec.encodeRow(cols, colIDs);
        
        // Decode the row
        Map<Long, byte[]> row = RowCodec.decodeRow(buf);
        
        // Verify each column
        for (int i = 0; i < colIDs.length; i++) {
            long id = colIDs[i];
            byte[] expected = cols[i];
            byte[] actual = row.get(id);
            
            assertNotNull("Column with ID " + id + " should not be null", actual);
            assertArrayEquals("Column values should match for ID " + id, expected, actual);
        }
    }

    @Test
    public void testEncodeDecodeVarint() throws Exception {
        // Test mix of positive and negative numbers
        long[] testValues = {-3L, -2L, -1L, 0L, 1L, 2L, 3L, -10L, 10L, -100L, 100L, -1024L, 1024L, 
            -102400, 102400L};
                
        for (long value : testValues) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            RowCodec.appendVarint(buf, value);
            byte[] encoded = buf.toByteArray();
            long decoded = RowCodec.decodeVarint(encoded, 0).value;
            assertEquals("Failed for mixed value: " + value, value, decoded);
        }
    }
}
