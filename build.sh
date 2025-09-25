#!/bin/bash

# YCSB OBHBase 编译打包脚本（增强版）
# 支持从 GitHub 拉取最新分支进行编译
set -e  # 遇到错误时退出

# 默认配置
USE_GITHUB=false
TABLE_URL="https://github.com/oceanbase/obkv-table-client-java.git"
TABLE_BRANCH="master"
HBASE_URL="https://github.com/oceanbase/obkv-hbase-client-java.git"
HBASE_BRANCH="hbase_2.0"
HBASE_USE_LOCAL_TABLE="true"

# 路径配置
BUILD_DIR="build"
PACKAGE_DIR="$(pwd)"
BUILD_PATH="$PACKAGE_DIR/$BUILD_DIR"
TEMP_PATH="$BUILD_PATH/temp"
BUILD_COMMITS_FILE="$BUILD_PATH/.BUILD_COMMITS"

# 显示帮助信息
show_help() {
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  clean                    清理构建产物和build目录"
    echo "  --use-github            从GitHub拉取最新分支进行编译"
    echo "  --table-branch BRANCH   指定table client分支 (默认: master)"
    echo "  --hbase-branch BRANCH   指定hbase client分支 (默认: hbase_2.0)"
    echo "  --no-local-table        不使用本地table client版本"
    echo "  -h, --help              显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                      # 标准编译"
    echo "  $0 clean                # 清理构建产物"
    echo "  $0 --use-github         # 从GitHub拉取最新代码编译"
    echo "  $0 --use-github --table-branch dev --hbase-branch feature"
    echo ""
    exit 0
}

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        clean)
            CLEAN_MODE=true
            shift
            ;;
        --use-github)
            USE_GITHUB=true
            shift
            ;;
        --table-branch)
            TABLE_BRANCH="$2"
            shift 2
            ;;
        --hbase-branch)
            HBASE_BRANCH="$2"
            shift 2
            ;;
        --no-local-table)
            HBASE_USE_LOCAL_TABLE="false"
            shift
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo "未知选项: $1"
            echo "使用 $0 --help 查看帮助信息"
            exit 1
            ;;
    esac
done

echo "=========================================="

# 检查Git是否安装（GitHub模式需要）
if [ "$USE_GITHUB" = "true" ] && ! command -v git &> /dev/null; then
    echo "错误：Git未安装或不在PATH中"
    echo "GitHub模式需要Git支持"
    exit 1
fi

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

echo "=========================================="
echo "YCSB OBHBase 编译打包脚本"
if [ "$CLEAN_MODE" = "true" ]; then
    echo "模式: 清理模式"
elif [ "$USE_GITHUB" = "true" ]; then
    echo "模式: GitHub 拉取编译"
    echo "Table Client 分支: $TABLE_BRANCH"
    echo "HBase Client 分支: $HBASE_BRANCH"
    echo "使用本地Table Client: $HBASE_USE_LOCAL_TABLE"
else
    echo "模式: 标准编译"
fi

# 处理清理模式
if [ "$CLEAN_MODE" = "true" ]; then
    echo "=========================================="
    echo "执行清理操作..."
    echo "=========================================="
    
    # 执行Maven清理
    echo "清理Maven构建产物..."
    mvn clean
    
    # 清理build目录
    if [ -d "$BUILD_PATH" ]; then
        echo "清理build目录..."
        rm -rf "$BUILD_PATH"
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

# GitHub模式：编译依赖客户端
if [ "$USE_GITHUB" = "true" ]; then
    echo "=========================================="
    echo "GitHub模式：开始编译依赖客户端..."
    echo "=========================================="
    
    # 创建build目录
    mkdir -p "$BUILD_PATH"
    
    # 初始化构建信息文件
    if [ -f "$BUILD_COMMITS_FILE" ]; then   
        rm -rf "$BUILD_COMMITS_FILE"
    fi
    
    # 编译Table Client
    compile_table_client() {
        echo "编译 Table Client..."
        cd "$TEMP_PATH"
        count=100
        set +e
        while [ $count -gt 1 ]; do
            count=$(( count - 1 ))
            rm -rf obkv-table-client-java
            echo "尝试克隆 Table Client 仓库 (剩余尝试次数: $count)..."
            git clone --depth 1 -b $TABLE_BRANCH $TABLE_URL
            if [ $? -eq 0 ]; then
                count=0
            fi
        done
        set -e
        
        if [ ! -d "obkv-table-client-java" ]; then
            echo "错误：无法克隆 Table Client 仓库"
            exit 1
        fi
        
        cd obkv-table-client-java
        TABLE_CLIENT_VERSION=$(grep -o '<version>.*</version>' pom.xml | head -n1 | sed 's/<version>\(.*\)<\/version>/\1/g')
        export TABLE_CLIENT_VERSION
        TABLE_COMMIT=$(git rev-parse HEAD)
        echo "Table Client 版本: $TABLE_CLIENT_VERSION, 提交: $TABLE_COMMIT"
        
        {
            echo ""
            echo "[TABLE CLIENT]"
            echo "REVISION: $TABLE_COMMIT"
            echo "BUILD_BRANCH: $TABLE_BRANCH"
            echo "BUILD_VERSION: $TABLE_CLIENT_VERSION"
            echo "BUILD_URL: $TABLE_URL"
        } >> "$BUILD_COMMITS_FILE"
        
        echo "开始编译 Table Client..."
        mvn clean package install -Dmaven.test.skip=true -Dgpg.skip=true -Dcheckstyle.skip=true
        echo "Table Client 编译完成"
    }
    
    # 编译HBase Client
    compile_hbase_client() {
        echo "编译 HBase Client..."
        cd "$TEMP_PATH"
        count=100
        set +e
        while [ $count -gt 1 ]; do
            count=$(( count - 1 ))
            rm -rf obkv-hbase-client-java
            echo "尝试克隆 HBase Client 仓库 (剩余尝试次数: $count)..."
            git clone --depth 1 -b $HBASE_BRANCH $HBASE_URL
            if [ $? -eq 0 ]; then
                count=0
            fi
        done
        set -e
        
        if [ ! -d "obkv-hbase-client-java" ]; then
            echo "错误：无法克隆 HBase Client 仓库"
            exit 1
        fi
        
        cd obkv-hbase-client-java
        HBASE_CLIENT_VERSION=$(grep -o '<version>.*</version>' pom.xml | head -n1 | sed 's/<version>\(.*\)<\/version>/\1/g')
        HBASE_COMMIT=$(git rev-parse HEAD)
        echo "HBase Client 版本: $HBASE_CLIENT_VERSION, 提交: $HBASE_COMMIT"
        
        {
            echo ""
            echo "[HBASE CLIENT]"
            echo "REVISION: $HBASE_COMMIT"
            echo "BUILD_BRANCH: $HBASE_BRANCH"
            echo "BUILD_VERSION: $HBASE_CLIENT_VERSION"
            echo "BUILD_URL: $HBASE_URL"
        } >> "$BUILD_COMMITS_FILE"
        
        echo "开始编译 HBase Client..."
        if [ "$HBASE_USE_LOCAL_TABLE" = "true" ]; then
            mvn clean package install -Dmaven.test.skip=true -Dgpg.skip=true -Dcheckstyle.skip=true -Dtable.client.version="$TABLE_CLIENT_VERSION"
        else
            mvn clean package install -Dmaven.test.skip=true -Dgpg.skip=true -Dcheckstyle.skip=true
        fi
        echo "HBase Client 编译完成"
    }
    
    # 创建临时目录并编译依赖
    echo "创建临时目录..."
    cd "$PACKAGE_DIR" && rm -rf "$TEMP_PATH" && mkdir -p "$TEMP_PATH"
    
    compile_table_client
    compile_hbase_client
    
    # 记录构建时间
    {
        echo ""
        echo "BUILD_TIME: $(date '+%b %d %Y %H:%M:%S')"
    } >> "$BUILD_COMMITS_FILE"
    
    echo "依赖客户端编译完成！"
    echo "=========================================="
fi

# 编译打包主项目
echo "开始编译打包主项目..."
cd $PACKAGE_DIR
if [ "$USE_GITHUB" = "true" ] && [ -n "$TABLE_CLIENT_VERSION" ] && [ -n "$HBASE_CLIENT_VERSION" ]; then
    echo "使用GitHub模式编译，指定客户端版本..."
    mvn clean install -DskipTests -Dcheckstyle.skip=true -Dmaven.test.skip=true -Dgpg.skip=true -Dtable.client.version="$TABLE_CLIENT_VERSION" -Dhbase.client.version="$HBASE_CLIENT_VERSION"
else
    echo "使用标准模式编译..."
    mvn clean install -DskipTests -Dcheckstyle.skip=true -Dmaven.test.skip=true -Dgpg.skip=true
fi

# 检查生成的jar包
JAR_PATH="obhbase/target/obhbase-1.0-SNAPSHOT-jar-with-dependencies.jar"
if [ -f "$JAR_PATH" ]; then
    echo "=========================================="
    echo "编译打包成功！"
    echo "生成的jar包位置：$JAR_PATH"
    echo "文件大小：$(ls -lh $JAR_PATH | awk '{print $5}')"
    echo "=========================================="
    
    # 创建输出目录
    if [ ! -d "$BUILD_PATH" ]; then
        echo "创建输出目录：$BUILD_PATH"
        mkdir -p "$BUILD_PATH"
    fi
    
    # 复制jar包到输出目录
    OUTPUT_JAR="$BUILD_PATH/obhbase-1.0-SNAPSHOT-jar-with-dependencies.jar"
    
    # 如果目标文件已存在，先重命名为.old后缀
    if [ -f "$OUTPUT_JAR" ]; then
        OLD_JAR="${OUTPUT_JAR}.old"
        echo "发现同名文件，重命名为：$OLD_JAR"
        mv "$OUTPUT_JAR" "$OLD_JAR"
    fi
    
    echo "复制jar包到：$OUTPUT_JAR"
    cp "$JAR_PATH" "$OUTPUT_JAR"
    
    # 复制构建信息文件（如果存在）
    if [ -f "$BUILD_COMMITS_FILE" ]; then
        echo "构建信息文件已存在：$BUILD_COMMITS_FILE"
    fi
    
    # 验证复制是否成功
    if [ -f "$OUTPUT_JAR" ]; then
        echo "=========================================="
        echo "Jar包复制成功！"
        echo "最终jar包位置：$OUTPUT_JAR"
        echo "文件大小：$(ls -lh $OUTPUT_JAR | awk '{print $5}')"
        
        if [ "$USE_GITHUB" = "true" ]; then
            echo ""
            echo "GitHub模式构建信息："
            if [ -f "$BUILD_COMMITS_FILE" ]; then
                cat "$BUILD_COMMITS_FILE"
            fi
        fi
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
