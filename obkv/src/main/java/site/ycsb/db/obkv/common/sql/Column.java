package site.ycsb.db.obkv.common.sql;

public class Column {
  @ColumnName("column_id")
  private Long columnId;
  @ColumnName("column_name")
  private String columnName;
  @ColumnName("rowkey_position")
  private Long rowKeyPosition;
  @ColumnName("data_type")
  private Long dataType;
  @ColumnName("data_length")
  private Long dataLength;
  @ColumnName("data_scale")
  private Long dataScale;
  @ColumnName("autoincrement")
  private Long autoIncrement;
  @ColumnName("column_flags")
  private Long columnFlags;
  @ColumnName("nullable")
  private Long nullable;
  @ColumnName("cur_default_value_v2")
  private byte[] curDefaultValueV2;
  @ColumnName("is_hidden")
  private Long isHidden;

  public Long getColumnId() {
    return columnId;
  }

  public void setColumnId(Long columnId) {
    this.columnId = columnId;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public Long getRowKeyPosition() {
    return rowKeyPosition;
  }

  public void setRowKeyPosition(Long rowKeyPosition) {
    this.rowKeyPosition = rowKeyPosition;
  }

  public Long getDataType() {
    return dataType;
  }

  public void setDataType(Long dataType) {
    this.dataType = dataType;
  }

  public Long getDataLength() {
    return dataLength;
  }

  public void setDataLength(Long dataLength) {
    this.dataLength = dataLength;
  }

  public Long getDataScale() {
    return dataScale;
  }

  public void setDataScale(Long dataScale) {
    this.dataScale = dataScale;
  }

  public Long getAutoIncrement() {
    return autoIncrement;
  }

  public void setAutoIncrement(Long autoIncrement) {
    this.autoIncrement = autoIncrement;
  }

  public Long getColumnFlags() {
    return columnFlags;
  }

  public void setColumnFlags(Long columnFlags) {
    this.columnFlags = columnFlags;
  }

  public Long getNullable() {
    return nullable;
  }

  public void setNullable(Long nullable) {
    this.nullable = nullable;
  }

  public byte[] getCurDefaultValueV2() {
    return curDefaultValueV2;
  }

  public void setCurDefaultValueV2(byte[] curDefaultValueV2) {
    this.curDefaultValueV2 = curDefaultValueV2;
  }

  public Long getIsHidden() {
    return isHidden;
  }

  public void setIsHidden(Long isHidden) {
    this.isHidden = isHidden;
  }
}
