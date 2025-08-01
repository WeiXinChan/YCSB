#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <max_key> <num_partitions>"
    exit 1
fi

MAX_KEY=$1
NUM_PARTITIONS=$2

# 计算每个分区的最大 key（按数字均分）
STEP=$(( MAX_KEY / NUM_PARTITIONS ))

# 默认使用 8 位固定长度的 key 格式
KEY_LENGTH=8

echo "CREATE TABLE test\$family (
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (K, Q, T)
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
