package site.ycsb.db.obkv.common.sql;

/**
 * <a href="https://github.com/oceanbase/oceanbase/blob/master/deps/oblib/src/common/object/ob_obj_type.h">Mysql模式具体类型参考这里</a>)
 */
public enum DataType {
  ObNullType(0),  //空类型
  ObTinyIntType(1),  // int8, aka mysql boolean type
  ObSmallIntType(2),  // int16
  ObMediumIntType(3),  // int24
  ObInt32Type(4),  // int32
  ObIntType(5),  // int64, aka bigint
  ObUTinyIntType(6),  // uint8
  ObUSmallIntType(7),  // uint16
  ObUMediumIntType(8),  // uint24
  ObUInt32Type(9),  // uint32
  ObUInt64Type(10), // uint64
  ObFloatType(11), // single-precision floating point
  ObDoubleType(12), // double-precision floating point
  ObUFloatType(13), // unsigned single-precision floating poin
  ObUDoubleType(14), // unsigned double-precision floating point
  ObNumberType(15), // aka decimal/numeric
  ObUNumberType(16),
  ObDateTimeType(17),
  ObTimestampType(18),
  ObDateType(19),
  ObTimeType(20),
  ObYearType(21),
  ObVarcharType(22), // charset: utf8mb4 or binary
  ObCharType(23), // charset: utf8mb4 or binary
  ObHexStringType(24), // hexadecimal literal, e.g. X'42', 0x42, b'1001', 0b1001
  ObExtendType(25), // Min, Max, NOP etc.
  ObUnknownType(26), // For question mark(?) in prepared statement, no need to serialize
  // @note future ne(ty)pes to be defined here !!!
  ObTinyTextType(27),
  ObTextType(28),
  ObMediumTextType(29),
  ObLongTextType(30),
  ObBitType(31),
  ObEnumType(32),
  ObSetType(33),
  ObEnumInnerType(34),
  ObSetInnerType(35),
  ObTimestampTZType(36), // timestamp with time zone for oracle
  ObTimestampLTZType(37), // timestamp with local time zone for oracle
  ObTimestampNanoType(38), // timestamp nanosecond for oracle
  ObRawType(39), // raw type for oracle
  ObIntervalYMType(40), // interval year to month
  ObIntervalDSType(41), // interval day to second
  ObNumberFloatType(42), // oracle float, subtype of NUMBER
  ObNVarchar2Type(43), // nvarchar2
  ObNCharType(44), // nchar
  ObURowIDType(45), // UROWID
  ObLobType(46), // Oracle Lob
  ObJsonType(47), // Json Type
  ObGeometryType(48), // Geometry type
  ObUserDefinedSQLType(49), // User defined type in SQL
  ObDecimalIntType(50),    // decimal int type
  ObCollectionSQLType(51), // collection(varray and nested table) in SQL
  ObMySQLDateType(52), // date type which is compatible with MySQL.
  ObMySQLDateTimeType(53), // datetime type which is compatible with MySQL.
  ObRoaringBitmapType(54), // Roaring Bitmap Type
  ObMaxType(55);          // invalid type, or count of obj type     // invalid type, or count of obj type

  private final long value;

  DataType(int value) {
    this.value = value;
  }

  public static DataType fromValue(long value) {
    for (DataType type : values()) {
      if (type.getValue() == value) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown value: " + value);
  }

  public long getValue() {
    return value;
  }
}
