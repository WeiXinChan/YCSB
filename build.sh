#!/bin/bash

# YCSB OBHBase 编译打包脚本
set -e  # 遇到错误时退出

echo "=========================================="
echo "开始编译打包 YCSB OBHBase..."
echo "=========================================="

# 检查Java是否安装
if ! command -v java &> /dev/null; then
    echo "错误：Java未安装或不在PATH中"
    echo "请先安装Java JDK"
    exit 1
fi

# 检查Java版本
JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1-2)
echo "检测到Java版本：$JAVA_VERSION"

# 检查Maven是否安装
if ! command -v mvn &> /dev/null; then
    echo "错误：Maven未安装或不在PATH中"
    echo "请先安装Maven"
    exit 1
fi

# 检查Maven版本
MAVEN_VERSION=$(mvn -version 2>&1 | head -n 1 | cut -d' ' -f3)
echo "检测到Maven版本：$MAVEN_VERSION"

# 检查是否为Maven 3
MAVEN_MAJOR_VERSION=$(echo "$MAVEN_VERSION" | cut -d'.' -f1)
if [ "$MAVEN_MAJOR_VERSION" != "3" ]; then
    echo "错误：需要Maven 3.x版本，当前版本为：$MAVEN_VERSION"
    echo "请升级到Maven 3.x版本"
    exit 1
fi

# 检查命令行参数
if [ "$1" = "clean" ]; then
    echo "=========================================="
    echo "执行清理操作..."
    echo "=========================================="
    
    # 执行Maven清理
    echo "清理Maven构建产物..."
    mvn clean
    
    # 清理build目录
    BUILD_DIR="build"
    if [ -d "$BUILD_DIR" ]; then
        echo "清理build目录..."
        rm -rf "$BUILD_DIR"
        echo "build目录已清理"
    else
        echo "build目录不存在，无需清理"
    fi
    
    echo "清理完成！"
    exit 0
fi

# 检查当前目录是否包含pom.xml
if [ ! -f "pom.xml" ]; then
    echo "错误：当前目录不包含pom.xml文件"
    echo "请确保在YCSB项目根目录下运行此脚本"
    exit 1
fi

# 清理之前的构建
echo "清理之前的构建..."
mvn clean

# 编译打包，跳过测试和checkstyle
echo "开始编译打包..."
mvn clean install -DskipTests -Dcheckstyle.skip=true -Dmaven.test.skip=true

# 检查生成的jar包
JAR_PATH="obhbase/target/obhbase-1.0-SNAPSHOT-jar-with-dependencies.jar"
if [ -f "$JAR_PATH" ]; then
    echo "=========================================="
    echo "编译打包成功！"
    echo "生成的jar包位置：$JAR_PATH"
    echo "文件大小：$(ls -lh $JAR_PATH | awk '{print $5}')"
    echo "=========================================="
    
    # 创建输出目录
    OUTPUT_DIR="build"
    echo "创建输出目录：$OUTPUT_DIR"
    mkdir -p "$OUTPUT_DIR"
    
    # 复制jar包到输出目录
    OUTPUT_JAR="$OUTPUT_DIR/obhbase-1.0-SNAPSHOT-jar-with-dependencies.jar"
    
    # 如果目标文件已存在，先重命名为.old后缀
    if [ -f "$OUTPUT_JAR" ]; then
        OLD_JAR="${OUTPUT_JAR}.old"
        echo "发现同名文件，重命名为：$OLD_JAR"
        mv "$OUTPUT_JAR" "$OLD_JAR"
    fi
    
    echo "复制jar包到：$OUTPUT_JAR"
    cp "$JAR_PATH" "$OUTPUT_JAR"
    
    # 验证复制是否成功
    if [ -f "$OUTPUT_JAR" ]; then
        echo "=========================================="
        echo "Jar包复制成功！"
        echo "最终jar包位置：$OUTPUT_JAR"
        echo "文件大小：$(ls -lh $OUTPUT_JAR | awk '{print $5}')"
        echo "=========================================="
        
    else
        echo "错误：jar包复制失败"
        exit 1
    fi
    
else
    echo "=========================================="
    echo "错误：未找到生成的jar包"
    echo "请检查编译过程是否有错误"
    echo "=========================================="
    exit 1
fi

echo "构建完成！" 