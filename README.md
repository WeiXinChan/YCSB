# YCSB测试工具
主要用于OBKV及其竞品的性能测试

## 目录结构

```
YCSB/
├── README.md                   # 项目说明文档
├── pom.xml                     # Maven主配置文件
├── core/                       # YCSB核心模块
│   ├── pom.xml                 # 核心模块Maven配置
│   ├── CHANGES.md              # 变更日志
│   └── src/                    # 源代码目录
│       ├── main/               # 主要源代码
│       └── test/               # 测试代码
│
├── obhbase/                    # OceanBase HBase兼容层测试模块
│   ├── README.md               # OBHBase模块说明
│   ├── pom.xml                 # Maven配置
│   ├── build.sh                # 构建脚本
│   ├── run_fast_test.sh        # 快速测试脚本
│   ├── create_partitioned_table.sh # 创建hbase range分区表脚本
│   ├── create_ts_partitioned_table.sh # 创建时序range分区表脚本
│   ├── src/                    # 源代码目录
│   └── workloads/              # 工作负载配置文件
│
└── obkv-table/                 # OBKV-Table测试模块
    ├── README                  # 模块说明
    ├── pom.xml                 # Maven配置
    ├── build.sh                # 构建脚本
    ├── run_fast_test.sh        # 快速测试脚本
    ├── create_partitioned_table.sh # 创建分区表脚本
    ├── src/                    # 源代码目录
    └── workloads/              # 工作负载配置文件
 
```

## 模块概要
每个测试模块的具体使用说明可以详见模块内的README.md文件:
- [obhbase](obhbase/README.md)
- [obkv-table](obkv-table/README.md)

### core/
YCSB框架的核心模块，引用自[YCSB项目](https://github.com/brianfrankcooper/YCSB)

### obhbase/
obhbase的性能测试模块，包含：
- OBHBase客户端实现
- 各种工作负载配置文件（PUT、READ、SCAN、批量操作等）
- 构建和测试脚本
- 测试使用说明（README）

### obkv-table/
OBKV-Table接口的性能测试模块，包含：
- OBKV-Table压测逻辑实现
- 对应的工作负载配置文件
- 构建和测试脚本
- 测试使用说明（README）