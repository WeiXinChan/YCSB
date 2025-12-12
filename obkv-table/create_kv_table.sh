#!/bin/bash

# =============================================================================
# KV表创建脚本 - 生成二级分区表SQL
# =============================================================================
# 
# 功能说明：
#   此脚本用于生成 KV 表的创建 SQL 语句，支持二级分区：
#   - 一级分区：基于 ts (timestamp) 的 RANGE 分区
#   - 二级分区：基于 pmid (CHAR(36)) 的 KEY 分区
#
# 参数说明：
#   $1 - range_partition_count: 一级range分区的数量（整数）
#   $2 - key_subpartition_count: 二级key分区的数量（整数）
#   $3 - start_timestamp: range分区的起始时间戳（毫秒时间戳或日期字符串，如：1704067200000 或 '2024-01-01 00:00:00'）
#   $4 - partition_duration_ms: 每个range分区的时间跨度（毫秒，如：31536000000 表示1年）
#
# 使用示例：
#   ./create_kv_table.sh 4 3 1704067200000 31536000000
#   ./create_kv_table.sh 4 3 '2024-01-01 00:00:00' 31536000000
#
# 输出说明：
#   脚本会输出完整的 CREATE TABLE 语句，包含：
#   - 表结构定义（pmid, ts, value）
#   - 一级分区定义（按 ts 字段范围分区）
#   - 二级分区定义（按 pmid 字段 KEY 分区）
#   - 每个一级分区的边界值（timestamp格式）
#
# 注意事项：
#   - 时间戳可以是毫秒时间戳（数字）或日期字符串（'YYYY-MM-DD HH:MM:SS'）
#   - 最后一个分区使用 MAXVALUE 作为上限
# =============================================================================

# 显示帮助信息
show_help() {
    echo "Usage: $0 <range_partition_count> <key_subpartition_count> <start_timestamp> <partition_duration_ms>"
    echo "   or: $0 -h"
    echo ""
    echo "Generate CREATE TABLE SQL for KV table with two-level partitioning:"
    echo "  - Level 1: RANGE partition by ts (timestamp)"
    echo "  - Level 2: KEY partition by pmid (CHAR(36))"
    echo ""
    echo "Parameters:"
    echo "  range_partition_count: 一级range分区的数量（整数）"
    echo "  key_subpartition_count: 二级key分区的数量（整数）"
    echo "  start_timestamp: 起始时间戳（毫秒时间戳或日期字符串，如：1704067200000 或 '2024-01-01 00:00:00'）"
    echo "  partition_duration_ms: 每个range分区的时间跨度（毫秒，如：31536000000 表示1年）"
    echo ""
    echo "Options:"
    echo "  -h, --help    Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 4 3 1704067200000 31536000000"
    echo "  $0 4 3 '2024-01-01 00:00:00' 31536000000"
    echo ""
    echo "Notes:"
    echo "  - Timestamp can be either a millisecond timestamp (number) or a date string"
    echo "  - The last partition uses MAXVALUE as the upper bound"
    echo "  - Each range partition will have the specified number of key subpartitions"
}

# 检查 -h 或 --help 选项
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
    exit 0
fi

if [ $# -ne 4 ]; then
    echo "Error: Invalid number of arguments"
    echo ""
    show_help
    exit 1
fi

RANGE_PARTITION_COUNT=$1
KEY_SUBPARTITION_COUNT=$2
START_TIMESTAMP=$3
PARTITION_DURATION_MS=$4

# 验证参数
if ! [[ "$RANGE_PARTITION_COUNT" =~ ^[0-9]+$ ]] || [ "$RANGE_PARTITION_COUNT" -le 0 ]; then
    echo "Error: range_partition_count must be a positive integer"
    exit 1
fi

if ! [[ "$KEY_SUBPARTITION_COUNT" =~ ^[0-9]+$ ]] || [ "$KEY_SUBPARTITION_COUNT" -le 0 ]; then
    echo "Error: key_subpartition_count must be a positive integer"
    exit 1
fi

if ! [[ "$PARTITION_DURATION_MS" =~ ^[0-9]+$ ]] || [ "$PARTITION_DURATION_MS" -le 0 ]; then
    echo "Error: partition_duration_ms must be a positive integer"
    exit 1
fi

# 将起始时间戳转换为毫秒时间戳
if [[ "$START_TIMESTAMP" =~ ^[0-9]+$ ]]; then
    # 已经是毫秒时间戳
    START_TS_MS=$START_TIMESTAMP
else
    # 尝试解析日期字符串
    if [[ "$START_TIMESTAMP" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2} ]]; then
        # 使用date命令转换为时间戳（秒），然后转换为毫秒
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            START_TS_SEC=$(date -j -f "%Y-%m-%d %H:%M:%S" "${START_TIMESTAMP}" +%s 2>/dev/null)
            if [ $? -ne 0 ]; then
                START_TS_SEC=$(date -j -f "%Y-%m-%d" "${START_TIMESTAMP}" +%s 2>/dev/null)
            fi
        else
            # Linux
            START_TS_SEC=$(date -d "${START_TIMESTAMP}" +%s 2>/dev/null)
        fi
        
        if [ $? -eq 0 ] && [ -n "$START_TS_SEC" ]; then
            START_TS_MS=$((START_TS_SEC * 1000))
        else
            echo "Error: Invalid timestamp format: $START_TIMESTAMP"
            echo "Please use either a millisecond timestamp (e.g., 1704067200000) or a date string (e.g., '2024-01-01 00:00:00')"
            exit 1
        fi
    else
        echo "Error: Invalid timestamp format: $START_TIMESTAMP"
        echo "Please use either a millisecond timestamp (e.g., 1704067200000) or a date string (e.g., '2024-01-01 00:00:00')"
        exit 1
    fi
fi

# 将毫秒时间戳转换为日期字符串（用于SQL）
convert_timestamp_to_date() {
    local ts_ms=$1
    local ts_sec=$((ts_ms / 1000))
    
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        date -r $ts_sec "+%Y-%m-%d %H:%M:%S" 2>/dev/null || date -j -f "%s" $ts_sec "+%Y-%m-%d %H:%M:%S"
    else
        # Linux
        date -d "@$ts_sec" "+%Y-%m-%d %H:%M:%S"
    fi
}

# 生成输出文件名
OUTPUT_FILE="kv_table_r${RANGE_PARTITION_COUNT}_k${KEY_SUBPARTITION_COUNT}.sql"

# 同时输出到控制台和文件的函数
output_both() {
    echo "$1" | tee -a "$OUTPUT_FILE"
}

# 清空输出文件（如果存在）
> "$OUTPUT_FILE"

# 写入参数信息到文件
output_both "-- ============================================================================="
output_both "-- KV Table Creation SQL"
output_both "-- Generated by: $0"
output_both "-- Generation time: $(date '+%Y-%m-%d %H:%M:%S')"
output_both "-- ============================================================================="
output_both ""
output_both "-- Parameters:"
output_both "--   Range partition count: $RANGE_PARTITION_COUNT"
output_both "--   Key subpartition count: $KEY_SUBPARTITION_COUNT"
output_both "--   Start timestamp: $START_TIMESTAMP (${START_TS_MS} ms)"
output_both "--   Partition duration: $PARTITION_DURATION_MS ms"
output_both ""
output_both "-- Start timestamp (formatted): $(convert_timestamp_to_date $START_TS_MS)"
output_both "-- Partition duration: $((PARTITION_DURATION_MS / 1000 / 60 / 60 / 24)) days"
output_both ""
output_both "-- ============================================================================="
output_both ""

# 生成表结构
output_both "CREATE TABLE kv_table ("
output_both "    pmid CHAR(36) NOT NULL,"
output_both "    ts TIMESTAMP(6) NOT NULL,"
output_both "    value DOUBLE,"
output_both "    PRIMARY KEY (pmid, ts)"
output_both ")"
output_both "PARTITION BY RANGE COLUMNS (ts)"
output_both "SUBPARTITION BY KEY(pmid) SUBPARTITIONS $KEY_SUBPARTITION_COUNT"
output_both "("

# 生成每个一级分区的定义
for ((i = 0; i < RANGE_PARTITION_COUNT; i++)); do
    # 计算当前分区的上限时间戳（毫秒）
    partition_end_ts_ms=$((START_TS_MS + (i + 1) * PARTITION_DURATION_MS))
    
    # 转换为日期字符串
    partition_end_date=$(convert_timestamp_to_date $partition_end_ts_ms)
    
    if [ $i -eq $((RANGE_PARTITION_COUNT - 1)) ]; then
        # 最后一个分区使用 MAXVALUE
        output_both "    PARTITION p$i VALUES LESS THAN (MAXVALUE)"
    else
        output_both "    PARTITION p$i VALUES LESS THAN ('$partition_end_date'),"
    fi
done

output_both ");"
output_both ""

# 只在控制台输出文件路径信息
echo ""
echo "SQL saved to: $OUTPUT_FILE"

