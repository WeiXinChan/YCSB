#!/bin/bash

# YCSB OBHBase 运行脚本
# 用途：运行YCSB测试，支持put、read、scan、load四种操作

set -e  # 遇到错误时退出

# 检查build目录是否存在
BUILD_DIR="build"
if [ ! -d "$BUILD_DIR" ]; then
    echo "错误：build目录不存在"
    echo "请先运行 ./build.sh 编译打包项目"
    exit 1
fi

# 检查jar包是否存在
JAR_FILE="$BUILD_DIR/obhbase-1.0-SNAPSHOT-jar-with-dependencies.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "错误：jar包不存在：$JAR_FILE"
    echo "请先运行 ./build.sh 编译打包项目"
    exit 1
fi

echo "=========================================="
echo "YCSB OBHBase 运行脚本"
echo "Jar包位置：$JAR_FILE"
echo "=========================================="

# 检查Java是否可用
if ! command -v java &> /dev/null; then
    echo "错误：Java未安装或不在PATH中"
    echo "请先安装Java"
    exit 1
fi

# 显示Java版本
JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1-2)
echo "检测到Java版本：$JAVA_VERSION"

# 检查命令行参数
if [ $# -eq 0 ]; then
    echo "用法：$0 {put|read|scan|load}"
    echo ""
    echo "选项说明："
    echo "  put   - 执行写入测试"
    echo "  read  - 执行读取测试"
    echo "  scan  - 执行扫描测试"
    echo "  load  - 执行read/scan数据的加载"
    exit 1
fi

OPERATION="$1"

# 根据操作类型执行相应的测试
case "$OPERATION" in
    "put")
        WORKLOAD_FILE="workloads/workload_put"
        if [ ! -f "$WORKLOAD_FILE" ]; then
            echo "错误：workload文件不存在：$WORKLOAD_FILE"
            exit 1
        fi
        echo "=========================================="
        echo "执行写入测试..."
        echo "=========================================="
        java -jar "$JAR_FILE" -P "$WORKLOAD_FILE"
        ;;
    "read")
        WORKLOAD_FILE="workloads/workload_read"
        if [ ! -f "$WORKLOAD_FILE" ]; then
            echo "错误：workload文件不存在：$WORKLOAD_FILE"
            exit 1
        fi
        echo "=========================================="
        echo "执行读取测试..."
        echo "=========================================="
        java -jar "$JAR_FILE" -P "$WORKLOAD_FILE"
        ;;
    "scan")
        WORKLOAD_FILE="workloads/workload_scan"
        if [ ! -f "$WORKLOAD_FILE" ]; then
            echo "错误：workload文件不存在：$WORKLOAD_FILE"
            exit 1
        fi
        echo "=========================================="
        echo "执行扫描测试..."
        echo "=========================================="
        java -jar "$JAR_FILE" -P "$WORKLOAD_FILE"
        ;;
    "load")
        WORKLOAD_FILE="workloads/workload_scan"
        if [ ! -f "$WORKLOAD_FILE" ]; then
            echo "错误：workload文件不存在：$WORKLOAD_FILE"
            exit 1
        fi
        echo "=========================================="
        echo "执行数据加载..."
        echo "=========================================="
        java -jar "$JAR_FILE" -P "$WORKLOAD_FILE" -load
        ;;
    *)
        echo "错误：不支持的操作类型：$OPERATION"
        echo "支持的操作：put, read, scan, load"
        exit 1
        ;;
esac

echo "=========================================="
echo "测试完成！"
echo "==========================================" 