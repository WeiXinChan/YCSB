#!/bin/bash
# 创建hbase的range-key二级分区表
# 用法: ./create_range_partitioned_table.sh [start_timestamp] [mills_duration] [range_partitions] [subpartitions]

# 显示帮助信息
show_help() {
    echo "用法: $0 [选项] [参数]"
    echo ""
    echo "参数:"
    echo "  start_timestamp    起始时间戳(毫秒)，默认为当前时间戳"
    echo "  mills_duration     每个range分区的时间间隔(毫秒)，默认86400000=(1天)"
    echo "  range_partitions   分区数量，默认7个"
    echo "  subpartitions      子分区数量，默认3个"
    echo ""
    echo "选项:"
    echo "  -h, --help         显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                                   # 使用默认参数"
    echo "  $0 1640995200000 7200000 12 4        # 自定义参数：range分区间隔7200000ms，分区数量12个，子分区数量4个"
    echo ""
    echo "说明:"
    echo "  - 该脚本生成obhbase的Range-key二级分区表创建语句"
    echo "  - 表名: usertable\$family"
    echo "  - 分区策略: RANGE COLUMNS(G) + KEY(K_PREFIX)子分区"
    echo "  - G列是T列的绝对值，K_PREFIX是K列的前16个字符"
    exit 0
}

# 检查帮助选项
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
fi

# 默认参数
START_TIMESTAMP=${1:-$(date +%s)000}  # 默认当前时间戳(毫秒)
MILLS_DURATION=${2:-86400000}          # 每个range分区的时间间隔，默认1天
RANGE_PART=${3:-7}                    # 默认7个range一级分区
SUBPARTITIONS=${4:-3}                 # 默认3个key二级分区

# 生成分区定义
PARTITION_DEFS=""
RANGE_DURATION_PART=$((RANGE_PART - 1))
for ((i=1; i<=RANGE_DURATION_PART; i++)); do
    PARTITION_VALUE=$((START_TIMESTAMP + MILLS_DURATION * i))
    if [ $i -eq $RANGE_DURATION_PART ]; then
        PARTITION_DEFS="${PARTITION_DEFS}  PARTITION \`p$((i-1))\` VALUES LESS THAN ($PARTITION_VALUE)"
    else
        PARTITION_DEFS="${PARTITION_DEFS}  PARTITION \`p$((i-1))\` VALUES LESS THAN ($PARTITION_VALUE),\n"
    fi
done

# 添加MAXVALUE分区
PARTITION_DEFS="${PARTITION_DEFS},\n  PARTITION \`p${RANGE_DURATION_PART}\` VALUES LESS THAN MAXVALUE"

# 生成完整的CREATE TABLE语句
CREATE_TABLE_SQL="CREATE TABLE IF NOT EXISTS \`usertable\$family\` (
  \`K\` varbinary(1024) NOT NULL,
  \`Q\` varbinary(256) NOT NULL,
  \`T\` bigint(20) NOT NULL,
  \`V\` varbinary(1024) DEFAULT NULL,
  \`G\` bigint(20) GENERATED ALWAYS AS (ABS(T)),
  \`K_PREFIX\` varbinary(1024) generated always as (substring(K, 1, 16)),
  PRIMARY KEY (\`K\`, \`Q\`, \`T\`)
)
PARTITION BY RANGE COLUMNS(\`G\`) SUBPARTITION BY KEY(\`K_PREFIX\`) SUBPARTITIONS ${SUBPARTITIONS} (
${PARTITION_DEFS}
);"

# 全局变量
OUTPUT_FILE="create_range_key_partitioned_table.txt"
echo "" > $OUTPUT_FILE
# print函数：同时输出到控制台和文件
print() {
    echo -e "$1"
    if [ -n "$OUTPUT_FILE" ]; then
        echo -e "$1" >> "$OUTPUT_FILE"
    fi
}

print "\n==============================================\n"
print "（重要！！！）起始时间戳(毫秒): $START_TIMESTAMP"
print "\n================================================\n"
print "表配置信息："
print "分区间隔(毫秒): $MILLS_DURATION"
print "分区数量: $RANGE_PART"
print "子分区数量: $SUBPARTITIONS"
print "生成的CREATE TABLE语句:"
print "\n==============================================\n"
print "$CREATE_TABLE_SQL"
print "\n==============================================\n"

