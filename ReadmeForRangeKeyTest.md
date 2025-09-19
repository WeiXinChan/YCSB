# HBase 二级分区 Range-Key 表测试指南

## 概述

本文档介绍如何使用 YCSB 对 obhbase的二级分区 Range-Key 表进行性能测试。该测试目前仅支持支持批量写入（batch put）、数据加载（load）和扫描（scan）三种操作。

## 目录结构

```
YCSB/
├── build.sh                                    # 编译脚本
├── run_range_key_test.sh                       # Range-Key表的测试运行脚本
├── create_range_key_partitioned_table.sh       # 创建 Range-Key分区表脚本
├── create_range_key_partitioned_table.txt      # 生成的建表语句文件
├── workloads/
│   ├── workload_batch_put_for_rk              # 批量写入测试配置
│   └── workload_scan_for_rk                   # 扫描测试配置
└── obhbase/src/main/java/com/oceanbase/obkv/ycsb/
    └── OBHBaseClient4RKPart.java              # Range-Key 专用客户端
```

## 环境准备

### 1. ob集群及obkv客户端要求

- observer版本需要大于等于4.3.5
- obproxy版本需要大于等于4.3.5
- observer集群需要在业务租户上开启分布式开关
```
alter system set _obkv_enable_distributed_execution=true;
```
- obkv-table及obkv-hbase客户端版本要求分别需要大于等2.0.0和2.2.0(hbase2x)或1.2.0(hbase1x)

### 2. 编译项目

```bash
# 编译并打包项目
./build.sh
```

编译成功后会在 `build/` 目录下生成 `obhbase-1.0-SNAPSHOT-jar-with-dependencies.jar` 文件。

## 测试步骤

### 1. 创建 Range-Key 二级分区表

#### 1.1 生成建表语句
- 使用默认参数生成：
  - `start_timestamp`: 起始时间戳（毫秒），默认为当前时间戳
  - `mills_duration`: 每个 range 分区的时间间隔（毫秒），默认1天
  - `range_partitions`: 分区数量，默认 7 个
  - `subpartitions`: 子分区数量，默认 3 个
```bash
./create_range_key_partitioned_table.sh
```
- 使用自定义参数
```
./create_range_key_partitioned_table.sh [start_timestamp] [mills_duration] [range_partitions] [subpartitions]
```
**注意：** 运行建表脚本后，会生成相应的记录文件（`create_range_key_partitioned_table.txt`），在后续的测试中，workload文件中的某些参数需要根据建表时指定的相关参数来填写

#### 1.2 执行建表语句

在测试的租户上建表：建表语句会生成在 `create_range_key_partitioned_table.txt` 文件中，类似如下：

```sql
CREATE TABLE IF NOT EXISTS `usertable$family` (
  `K` varbinary(1024) NOT NULL,
  `Q` varbinary(256) NOT NULL,
  `T` bigint(20) NOT NULL,
  `V` varbinary(1024) DEFAULT NULL,
  `G` bigint(20) GENERATED ALWAYS AS (ABS(T)),
  `K_PREFIX` varbinary(1024) generated always as (substring(K, 1, 16)),
  PRIMARY KEY (`K`, `Q`, `T`)
)
PARTITION BY RANGE COLUMNS(`G`) SUBPARTITION BY KEY(`K_PREFIX`) SUBPARTITIONS 3 (
  PARTITION `p0` VALUES LESS THAN (1758272006000),
  PARTITION `p1` VALUES LESS THAN (1758275606000),
  ...
  PARTITION `p7` VALUES LESS THAN MAXVALUE
);
```

### 2. 配置测试参数
下面的参数配置都在workload文件中设置。配置的类型分成下面的类别来介绍：

#### 2.1 连接配置

分别在 `workloads/workload_scan_for_rk` 和 `workloads/workload_batch_put_for_rk` 中配置数据库连接：

```properties
# ODP 模式连接（推荐）
hbase.oceanbase.odpMode=true
hbase.oceanbase.odpAddr=
hbase.oceanbase.odpPort=

# 或直连模式
hbase.oceanbase.odpMode=false
hbase.oceanbase.paramURL=
hbase.oceanbase.sysUserName=
hbase.oceanbase.sysPassword=

# 租户用户配置
hbase.oceanbase.fullUserName=
hbase.oceanbase.password=

# 数据库和表配置
hbase.oceanbase.database=test
hbase.oceanbase.table=usertable
hbase.oceanbase.columnFamily=family
```
#### 2.2 YCSB测试参数
##### 2.3.0 公共参数
**YCSB自带的配置：**
- recordcount：load操作生成的总操作数
- operationcount：总操作数（这里对应batch_put和scan对应生成的总操作数）
- fieldcount：单次写入的列数
- fieldlength：每列的长度
- threadcount：压测线程数
- requestdistribution：生成的请求分布类型

**针对range-key二级分区表新增的配置：**
- current.mill：起始时间戳，所有请求的T都基于这个生成
- table.range.mills：range分区的时间间隔（ms）
- range.uid.count：每个range分区容纳的最大uid数量
- total.uid.count：生成uid的总数

##### 2.3.1 batch_put测试
- batchputproportion：batch_put的操作比例
- batchput.size.per.op：每个batch_put操作的op数
- batchput.issamepart.per.op：控制每个batch请求里是否是同个分区（true or false）

##### 2.3.2 scan测试
**load阶段生效的配置：**
- recordcount：生成的总数据量会约等于recordcount * batchput.size.per.op
- load.use.batchput：使用batch_put进行load（在这个测试中建议为true）
- batchput.size.per.op：batch_put的op数
- batchput.issamepart.per.op：batch操作中是否是同个分区操作（在这个测试中建议为true）

**scan阶段生效的配置：**
- scan.rows：扫描的记录数
- table.scan.one.part：扫描一个range分区还是所有range分区

**举例：**
以下面的一个配置说明这些配置组合后达到的效果：
```
# load
recordcount=300000000
fieldcount=5
fieldlength=500
load.use.batchput=true
batchput.size.per.op=2
batchput.issamepart.per.op=true
range.uid.count=14000
total.uid.count=100000
# scan
scan.rows=200
table.scan.one.part=false
```
- 生成3亿个batch操作，每个batch2个操作，生成6亿条数据，每条5列，每列500B，单条2.5KB，总共约1.3T数据
- 假设数据均匀分布在每个分区上：
  - 每个一级range分区会有1.4w个uid，总共会约有10w个uid分布在7个range一级分区
    - 这样相当于每个uid在一个range分区（代表一天）里大约会有6000条记录；

#### 2.3 客户端参数
**查询参数**
rpc.operation.timeout=10000
rpc.execute.timeout=15000

**连接池**
server.connection.pool.size=50

### 3. 运行测试

#### 3.1 批量写入测试

```bash
# 执行批量写入测试
./run_range_key_test.sh batch_put
```

#### 3.2 scan测试数据加载

```bash
# 加载测试数据
./run_range_key_test.sh load
```

#### 3.3 扫描测试

```bash
# 执行扫描测试
./run_range_key_test.sh scan
```