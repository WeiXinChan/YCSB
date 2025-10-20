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
#   -t|--timeseries: 创建时序表（可选标志）
#
# 使用示例：
#   ./create_partitioned_table.sh 1000 4
#   ./create_partitioned_table.sh 10000 8 16
#   ./create_partitioned_table.sh 100000 16 8
#   ./create_partitioned_table.sh -t 1000 4
#   ./create_partitioned_table.sh --timeseries 10000 8 16
#
# 输出说明：
#   脚本会输出完整的 CREATE TABLE 语句，包含：
#   普通表模式：
#     - 表结构定义（K, Q, T, V 字段）
#     - 主键：(K, Q, T)
#   时序表模式（使用 -t 或 --timeseries）：
#     - 表结构定义（K, T, S, V 字段）
#     - 主键：(K, T, S)
#     - V 字段类型为 json
#   共同特性：
#     - 分区定义（按 K 字段范围分区）
#     - 每个分区的边界值（使用零填充的固定长度格式）
#
# 注意事项：
#   - 分区边界使用字符串格式，支持按字典序排序
#   - 最后一个分区使用 MAXVALUE 作为上限
#   - key 值会被填充到指定长度（默认12位）
# =============================================================================

# 默认为普通表模式
TIMESERIES_MODE=false

# 检查是否有时序表标志
if [ "$1" = "-t" ] || [ "$1" = "--timeseries" ]; then
    TIMESERIES_MODE=true
    shift
fi

if [ $# -lt 2 ] || [ $# -gt 3 ]; then
    echo "Usage: $0 [-t|--timeseries] <max_key> <num_partitions> [key_length]"
    echo "  -t, --timeseries: create timeseries table (K, T, S, V with PRIMARY KEY (K, T, S))"
    echo "  key_length: optional, default is 12"
    exit 1
fi

MAX_KEY=$1
NUM_PARTITIONS=$2
KEY_LENGTH=${3:-12}  # 默认为12

# 计算每个分区的最大 key（按数字均分）
STEP=$(( MAX_KEY / NUM_PARTITIONS ))

# 根据模式输出不同的表结构
if [ "$TIMESERIES_MODE" = true ]; then
    echo "CREATE TABLE test\$family (
    K varbinary(1024) NOT NULL,
    T bigint(20) NOT NULL,
    S bigint(20) NOT NULL,
    V json DEFAULT NULL,
    PRIMARY KEY (K, T, S)
)
PARTITION BY RANGE COLUMNS(K) ("
else
    echo "CREATE TABLE test\$family (
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (K, Q, T)
)
PARTITION BY RANGE COLUMNS(K) ("
fi

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
