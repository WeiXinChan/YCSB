# YCSB Obkv-Table 测试工具

YCSB (Yahoo! Cloud System Benchmark) 是一个用于测试云数据库性能的基准测试工具。本项目是YCSB的OBKV-Table绑定版本，支持测试OceanBase Table模型的性能测试。

## 项目结构

```
obkv-table/                     # YCSB核心模块
├── src/                        # OBKV-Table数据库
├── workloads/                  # 工作负载配置文件
├── build.sh                    # 编译打包脚本
├── run_fast_test.sh            # 运行测试脚本
└── build/                      # 构建输出目录
    └── obkv-table-1.0-SNAPSHOT-jar-with-dependencies.jar
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

##### OceanBase连接参数

| 参数名 | 说明 | 必填 |
|--------|------|------|
| `obkv.isOdpMode` | 连接模式选择 | 是 |
| `obkv.odpAddr` | ODP代理地址 | ODP模式必填 |
| `obkv.odpPort` | ODP代理端口 | ODP模式必填 |
| `obkv.configURL` | 直连模式连接URL | 直连模式必填 |
| `obkv.sysUserName` | 系统租户用户名 | 直连模式必填 |
| `obkv.sysPassword` | 系统租户密码 | 直连模式必填 |
| `obkv.fullUserName` | 业务租户用户名 | 是 |
| `obkv.password` | 业务租户密码 | 是 |
| `obkv.database` | 数据库名 | 是 |
**注意：** 其他客户端参数设置，可以参考obkv-table-client支持的参数设置

##### YCSB测试通用参数

| 参数名 | 说明 | 默认值 |
|--------|------|--------|
| `operationcount` | 操作总数 | - |
| `recordcount` | 记录总数 | - |
| `requestdistribution` | 请求分布模式 | uniform |
| `obkv.insertType` | insert操作使用的obkv接口| insertup |
| `obkv.updateType` | update操作使用的obkv接口| update |
| `obkv.batchPutType`| batch_put操作使用的obkv接口 | put |

注意：没有指定insertstart和insertcount的情况下
- load测试载入数据的起点是从0开始

### 4. 配置示例

适用于OceanBase数据库的配置：

```properties
# 直连模式配置
obkv.isOdpMode=false
obkv.configURL=
obkv.sysUserName=
obkv.sysPassword=

# odp模式配置
obkv.isOdpMode=true
obkv.odpAddr=
obkv.odpPort=
# 账密
obkv.fullUserName=
obkv.password=

```

### 5. 快速运行测试
#### 5.0. 预建表
**注意：** 如果需要测试scan操作，需要预建range分区表，key分区表的scan操作会进行全表扫描
```
# 预建range分区表
sh create_partitioned_table.sh 30000000 20 12
```
`create_partitioned_table.sh`会生成测试需要的建表语句，需要使用sql客户端进行建表：
```
CREATE TABLE usertable (
    ycsb_key varbinary(1024) NOT NULL,
    field0 varbinary(1024) NOT NULL,
    PRIMARY KEY (ycsb_key)
)
PARTITION BY RANGE COLUMNS(ycsb_key) (
    PARTITION p0 VALUES LESS THAN ('000000050000'),
    PARTITION p1 VALUES LESS THAN ('000000100000'),
    PARTITION p2 VALUES LESS THAN ('000000150000'),
    PARTITION p3 VALUES LESS THAN ('000000200000'),
    PARTITION p4 VALUES LESS THAN ('000000250000'),
    PARTITION p5 VALUES LESS THAN ('000000300000'),
    PARTITION p6 VALUES LESS THAN ('000000350000'),
    PARTITION p7 VALUES LESS THAN ('000000400000'),
    PARTITION p8 VALUES LESS THAN ('000000450000'),
    PARTITION p9 VALUES LESS THAN ('000000500000'),
    PARTITION p10 VALUES LESS THAN ('000000550000'),
    PARTITION p11 VALUES LESS THAN ('000000600000'),
    PARTITION p12 VALUES LESS THAN ('000000650000'),
    PARTITION p13 VALUES LESS THAN ('000000700000'),
    PARTITION p14 VALUES LESS THAN ('000000750000'),
    PARTITION p15 VALUES LESS THAN ('000000800000'),
    PARTITION p16 VALUES LESS THAN ('000000850000'),
    PARTITION p17 VALUES LESS THAN ('000000900000'),
    PARTITION p18 VALUES LESS THAN ('000000950000'),
    PARTITION p19 VALUES LESS THAN (MAXVALUE)
);
```
#### 5.1. 进行scan测试
```bash
# 先导入数据，这一步会交互式地让用户选择load哪种测试的数据（read/scan/batchread），会分别对应取读对应的workload文件
./run_fast_test.sh load

# 如果想使用自己自定义的workload文件，可以显式指定
./run_fast_test.sh load workloads/my_workload

# 确定每个分区的key数量是否均衡
select count(1) from usertable partition(p0);
select count(1) from usertable partition(p1);
select count(1) from usertable partition(p2);
...

# 扫描数据
./run_fast_test.sh scan

# 超时参数调优，在workload文件里指定设置
rpc.operation.timeout=10000 # 服务端执行的超时时间
rpc.execute.timeout=15000   # 客户端等待请求返回的超时时间
```

#### 5.2. 进行put测试
```bash
# 如果需要先load数据
./run_fast_test.sh load workloads/workload_put
# 运行put测试
./run_fast_test.sh put
```
**注意**：默认使用obkv的insertup接口，如果需要使用其他接口，在workload文件上通过设置`obkv.insertType=`，例如：
```bash
# 使用insert接口，插入数据重复会报错
obkv.insertType=insert
# 使用put接口，覆盖写，插入数据重复会覆盖（需要在server上将binlog_row_image变量设置为minimal，不支持全列日志）
mysql> set global binlog_row_image='MINIMAL';
# workload文件里指定
obkv.insertType=put
# 默认：使用insertup，插入数据重复会转成update操作
obkv.insertType=insertup
```
#### 5.3. 进行batch_put测试
```bash
# 测试batchput时，默认使用的是put接口（即：obkv.batchPutType=put)，binlog只支持minimal镜像，此时需要将server端的binlog_row_image变量设置为minimal
# 登录业务租户，执行如下命令
mysql> set global binlog_row_image='MINIMAL';
# 运行batch_put测试
./run_fast_test.sh batch_put
```
**注意**：默认使用obkv的put接口，如果需要使用其他接口，在workload文件上通过设置`obkv.batchPutType=`，例如：
```bash
# 默认：使用put接口，覆盖写，插入数据重复会覆盖（需要在server上将binlog_row_image变量设置为minimal，不支持全列日志）
obkv.insertType=put
# 使用insert接口，插入数据重复会报错
obkv.insertType=insert
# 使用insertup，插入数据重复会转成update操作
obkv.insertType=insertup
```
#### 5.4. 进行read测试
```bash
# 同scan，需要先load数据
./run_fast_test.sh load

# 运行
./run_fast_test.sh read
```
#### 5.5. 进行batch_read测试
```bash
# 同scan，需要先load数据
./run_fast_test.sh load

# 运行
./run_fast_test.sh batch_read
```

#### 5.6. 进行自定义workload的测试
```bash
# 1. 自定义一个worload文件，位置在/path/to/custom/workload
# 1.1 （可选）如果需要提前载入数据，使用这个文件进行数据加载
./run_fast_test.sh load /path/to/custom/workload
# 2. 运行这个测试
./run_fast_test.sh worload /path/to/custom/workload
```
