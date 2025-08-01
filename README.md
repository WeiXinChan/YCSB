# YCSB OBHBase 测试工具

YCSB (Yahoo! Cloud System Benchmark) 是一个用于测试云数据库性能的基准测试工具。本项目是YCSB的OBHBase绑定版本，用于测试OceanBase HBase兼容模式的性能。

## 项目结构

```
YCSB/
├── core/                       # YCSB核心模块
├── obhbase/                    # OBHBase绑定模块
├── workloads/                  # 工作负载配置文件
├── build.sh                    # 编译打包脚本
├── run.sh                      # 运行测试脚本
|—— create_partitioned_table.sh # 创建预分区表脚本
└── build/                      # 构建输出目录
    └── obhbase-1.0-SNAPSHOT-jar-with-dependencies.jar
```

## 快速开始

### 1. 环境要求

- **Java**: JDK 1.7 或更高版本
- **Maven**: 3.x 版本
- **操作系统**: Linux/macOS/Windows

### 2. 编译打包

```bash
# 编译打包项目
./build.sh

# 仅清理构建产物
./build.sh clean
```

### 3. 配置workload

在使用前，需要配置workload文件中的OceanBase连接参数。编辑对应的workload文件（如`workloads/workload_scan`）：
- scan: workloads/workload_scan

```bash
# 编辑workload文件
vim workloads/workload_scan
```

#### OceanBase连接参数（必填）

| 参数名 | 说明 | 必填 |
|--------|------|------|
| `hbase.oceanbase.odpMode` | 连接模式选择 | 是 |
| `hbase.oceanbase.odpAddr` | ODP代理地址 | ODP模式必填 |
| `hbase.oceanbase.odpPort` | ODP代理端口 | ODP模式必填 |
| `hbase.oceanbase.paramURL` | 直连模式连接URL | 直连模式必填 |
| `hbase.oceanbase.sysUserName` | 系统租户用户名 | 直连模式必填 |
| `hbase.oceanbase.sysPassword` | 系统租户密码 | 直连模式必填 |
| `hbase.oceanbase.fullUserName` | 业务租户用户名 | 是 |
| `hbase.oceanbase.password` | 业务租户密码 | 是 |
| `hbase.oceanbase.database` | 数据库名 | 是 |
| `hbase.oceanbase.table` | 表名 | 是 |
| `hbase.oceanbase.columnFamily` | 列族名 | 是 |
PS:其他客户端参数设置，可以参考obkv-hbase-client和obkv-table-client支持的参数设置

#### YCSB测试参数

| 参数名 | 说明 | 默认值 |
|--------|------|--------|
| `operationcount` | 操作总数 | - |
| `recordcount` | 记录总数 | - |
| `requestdistribution` | 请求分布模式 | uniform |

注意：没有指定insertstart和insertcount的情况下
- load测试载入数据的起点是从0开始

### 4. 预建表
#### 使用 create_partitioned_table.sh 脚本生成建表语句
```bash
# 记录数有 3 千万, 分区数 20 个
./create_partitioned_table.sh 30000000 20
```

#### 在 OceanBase 数据库中执行上述脚本生成的建表语句
```bash
CREATE TABLE test$family (
    K varbinary(1024) NOT NULL,
    Q varbinary(256) NOT NULL,
    T bigint(20) NOT NULL,
    V varbinary(1024) DEFAULT NULL,
    PRIMARY KEY (K, Q, T)
)
PARTITION BY RANGE COLUMNS(K) (
    PARTITION p0 VALUES LESS THAN ('01500000'),
    PARTITION p1 VALUES LESS THAN ('03000000'),
    PARTITION p2 VALUES LESS THAN ('04500000'),
    PARTITION p3 VALUES LESS THAN ('06000000'),
    PARTITION p4 VALUES LESS THAN ('07500000'),
    PARTITION p5 VALUES LESS THAN ('09000000'),
    PARTITION p6 VALUES LESS THAN ('10500000'),
    PARTITION p7 VALUES LESS THAN ('12000000'),
    PARTITION p8 VALUES LESS THAN ('13500000'),
    PARTITION p9 VALUES LESS THAN ('15000000'),
    PARTITION p10 VALUES LESS THAN ('16500000'),
    PARTITION p11 VALUES LESS THAN ('18000000'),
    PARTITION p12 VALUES LESS THAN ('19500000'),
    PARTITION p13 VALUES LESS THAN ('21000000'),
    PARTITION p14 VALUES LESS THAN ('22500000'),
    PARTITION p15 VALUES LESS THAN ('24000000'),
    PARTITION p16 VALUES LESS THAN ('25500000'),
    PARTITION p17 VALUES LESS THAN ('27000000'),
    PARTITION p18 VALUES LESS THAN ('28500000'),
    PARTITION p19 VALUES LESS THAN (MAXVALUE)
);
```

### 5. 快速运行测试

```bash
# 先导入数据
./run_fast_test.sh load ob-hbase -P workloads/workload_scan

# 确定每个分区的key数量是否均衡
select count(1) from test$family partition(p0);
select count(1) from test$family partition(p1);
select count(1) from test$family partition(p2);
...

# 扫描数据
./run_fast_test.sh scan ob-hbase -P workloads/workload_scan
```

