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
###### OceanBase连接参数（OBKV模式必填

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
**注意：** 其他客户端参数设置，可以参考obkv-hbase-client和obkv-table-client支持的参数设置

##### YCSB测试通用参数
** 重要提示：** workload类型要设置为TYWorkload，即：在workload文件中指定：
```
workload=com.oceanbase.obkv.ycsb.TYWorkload
```
| 参数名 | 说明 | 默认值 |
|--------|------|--------|
| `operationcount` | 操作总数 | - |
| `recordcount` | 记录总数 | - |
| `requestdistribution` | 请求分布模式 | uniform |

### 4. 配置示例

#### 4.1 OBKV模式配置示例


```properties
# !!!重要!!!
# 设置workload为TYWorkload
workload=com.oceanbase.obkv.ycsb.TYWorkload

# 连接模式
isObkv=true
debug=false

# 直连模式配置
hbase.oceanbase.odpMode=false
hbase.oceanbase.paramURL=
hbase.oceanbase.sysUserName=
hbase.oceanbase.sysPassword=
hbase.oceanbase.fullUserName=
hbase.oceanbase.password=

# 表配置
hbase.oceanbase.database=test
hbase.oceanbase.table=test
hbase.oceanbase.columnFamily=cf
```

### 5. 预建表
- 时序模式用：
```
./create_ts_partitioned_table.sh <recordcount> <partitioned num>
```
- hbase模式用：
```
./create_partitioned_table.sh <recordcount> <partitioned num>
```

### 6. 快速运行测试
#### 6.1. 进行scan测试
```bash
# 先导入数据，这一步会交互式地让用户选择load哪种测试的数据（read/scan/batchread），会分别对应取读对应的workload文件
./run_fast_test.sh load

# 如果想使用自己自定义的workload文件，可以显式指定
./run_fast_test.sh load workloads/my_workload

# 确定每个分区的key数量是否均衡
select count(1) from test$family partition(p0);
select count(1) from test$family partition(p1);
select count(1) from test$family partition(p2);

# 扫描数据
./run_fast_test.sh scan
```
#### 6.2. 进行put测试
```bash
# 运行put测试
./run_fast_test.sh put
```
#### 6.3. 进行batch_put测试
```bash
# 运行batch_put测试
./run_fast_test.sh batch_put
```
#### 6.4. 进行read测试
```bash
# 同上，需要先load数据
./run_fast_test.sh load

# 运行
./run_fast_test.sh read
```
#### 6.5. 进行batch_read测试
```bash
# 同上，需要先load数据
./run_fast_test.sh load

# 运行
./run_fast_test.sh batch_read
```

#### 6.6. 进行自定义workload的测试
```bash
# 1. 自定义一个worload文件，位置在/path/to/custom/workload
# 1.1 （可选）如果需要提前载入数据，使用这个文件进行数据加载
./run_fast_test.sh load /path/to/custom/workload
# 2. 运行这个测试
./run_fast_test.sh worload /path/to/custom/workload
```