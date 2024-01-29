<!--
Copyright (c) 2014 - 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on OBKV. 

### 1. Start observer

### 2. Install Java and Maven

### 3. Set Up YCSB

compile:

    mvn -pl site.ycsb:obkv-binding -am clean package

### 4. Provide OBKV Connection Parameters
    
Set obkv.fullUserName, obkv.configUrl, obkv.password, and so on in the workload you plan to run.

### 5. Load data and run tests

Load the data:

    bin/ycsb load obkv -P workloads/workload_obkv -s -thread 100

Run the workload test:

    bin/ycsb run obkv -P workloads/workload_obkv -s -thread 100

