#!/bin/bash

# =============================================================================
# YCSB 分区表创建脚本
# =============================================================================
# 
# 功能说明：
#   此脚本用于生成 YCSB 测试的分区表创建 SQL 语句。
#   根据指定的最大 key 值和分区数量，自动计算每个分区的范围，
#   并生成相应的 PARTITION BY RANGE COLUMNS 语句。
#
# 参数说明：
#   $1 - max_key: 最大 key 值（整数）
#   $2 - num_partitions: 分区数量（整数）
#   $3 - key_length: key 的固定长度（可选，默认12位）
#
# 使用示例：
#   ./create_ts_partitioned_table.sh 1000 4
#   ./create_ts_partitioned_table.sh 10000 8 16
#   ./create_ts_partitioned_table.sh 100000 16 8
#
# 输出说明：
#   脚本会输出完整的 CREATE TABLE 语句，包含：
#   - 表结构定义（K, T, S, V 字段）
#   - 分区定义（按 K 字段范围分区）
#   - 每个分区的边界值（使用零填充的固定长度格式）
#
# 注意事项：
#   - 分区边界使用字符串格式，支持按字典序排序
#   - 最后一个分区使用 MAXVALUE 作为上限
#   - key 值会被填充到指定长度（默认12位）
# =============================================================================

if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Usage: $0 <max_key> <num_partitions> [key_length]"
    echo "  key_length: optional, default is 12"
    exit 1
fi

MAX_KEY=$1
NUM_PARTITIONS=$2
KEY_LENGTH=${3:-12}  # 默认为12

# 计算每个分区的最大 key（按数字均分）
STEP=$(( MAX_KEY / NUM_PARTITIONS ))

echo "CREATE TABLE test\$ts_family (
    K varbinary(1024) NOT NULL,
    T bigint(20) NOT NULL,
    S bigint(20) NOT NULL,
    V json DEFAULT NULL,
    PRIMARY KEY (K, T, S)
)
PARTITION BY RANGE COLUMNS(K) ("

for ((i = 0; i < NUM_PARTITIONS; i++)); do
    # 计算当前分区的上限 key
    upper_bound=$(( STEP * (i + 1) ))
    padded_upper=$(printf "%0${KEY_LENGTH}d" $upper_bound)

    if [ $i -eq $((NUM_PARTITIONS - 1)) ]; then
        echo "    PARTITION p$i VALUES LESS THAN (MAXVALUE)"
    else
        echo "    PARTITION p$i VALUES LESS THAN ('$padded_upper'),"
    fi
done

echo ");"
