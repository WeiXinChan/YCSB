package com.oceanbase.obkv.ycsb;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.workloads.CoreWorkload;
import java.util.*;


public class TYWorkload extends CoreWorkload {
  Random random = new Random();
  
  @Override
  public HashMap<String, ByteIterator> buildValues(String key) {
    HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
    /*{
      "devId": "6c28c3ee4bdd67e9xxxxx",
      "eventTime": "1756446799264",
      "eventId": "7",
      "requestFrom": "1",
      "eventName": "datapoint_report",
      "env": "1",
      "row": "0023db24avdt769e76ddb4ee3c82c6_9223370280407976543_7_18",
      "dpId": "18",
      "dpValue": "AQEAZAMBARMEAQCv",
      "status": "1",
      "pid": "woshipidxxx",
      "messageId": "00063D7AA3896E7CC988406167AE5D3E",
      "freeESFlag": "false"
    }*/
    // devId
    values.put("devId", new StringByteIterator(generateRandomString(32)));
    // eventTime
    values.put("eventTime", new StringByteIterator(generateTimestamp()));
    // eventId
    values.put("eventId", new StringByteIterator(generateRandomNumber(1)));
    // requestFrom
    values.put("requestFrom", new StringByteIterator(generateRandomNumber(1)));
    // eventName
    values.put("eventName", new StringByteIterator(generateRandomString(20)));
    // env
    values.put("env", new StringByteIterator(generateRandomNumber(1)));
    // row
    values.put("row", new StringByteIterator(generateRandomString(100)));
    // dpId
    values.put("dpId", new StringByteIterator(generateRandomNumber(2)));
    // dpValue
    values.put("dpValue", new StringByteIterator(generateRandomString(20)));
    // status
    values.put("status", new StringByteIterator(generateRandomNumber(1)));
    // pid
    values.put("pid", new StringByteIterator(generateRandomString(12)));
    // messageId
    values.put("messageId", new StringByteIterator(generateRandomString(32)));
    // freeESFlag
    values.put("freeESFlag", new StringByteIterator(generateRandomBoolean()));
    return values;
  }

  private String generateRandomString(int length) {
    if (length <= 0) {
      return "";
    }
    
    // 使用char数组而不是StringBuilder，减少内存分配
    char[] chars = new char[length];
    
    // 预定义字符集，避免重复创建
    final char[] charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();
    final int charSetLength = charSet.length;
    
    // 使用ThreadLocalRandom提高性能，避免synchronized
    java.util.concurrent.ThreadLocalRandom random = java.util.concurrent.ThreadLocalRandom.current();
    
    // 批量生成随机字符，减少方法调用开销
    for (int i = 0; i < length; i++) {
      chars[i] = charSet[random.nextInt(charSetLength)];
    }
    
    return new String(chars);
  }

  private String generateTimestamp() {
    return String.valueOf(System.currentTimeMillis());
  }

  private String generateRandomNumber(int length) {
    if (length <= 0) {
      return "";
    }
    return String.valueOf(random.nextInt(length));
  }

  private String generateRandomBoolean() {
    return random.nextBoolean() ? "true" : "false";
  }

}
