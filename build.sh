#!/usr/bin/env bash

package_dir=$(pwd)
table_url="git@github.com:oceanbase/obkv-table-client-java.git"
table_branch="master"
hbase_url="git@github.com:oceanbase/obkv-hbase-client-java.git"
hbase_branch="hbase_2.0"
hbase_use_local_table="${1:-true}"

# table client
function compileTable(){
    cd "$package_dir"/temp
    count=100
    set +e
    while [ $count -gt 1 ]
    do
        count=$(( count - 1 ))
        rm -rf obkv-table-client-java
        git clone --depth 1 -b $table_branch $table_url
        if [ $? -eq 0 ]; then
            count=0
        fi
    done
    set -e
    cd obkv-table-client-java
    TABLE_CLIENT_VERSION=$(grep -o '<version>.*</version>' pom.xml | head -n1 | sed 's/<version>\(.*\)<\/version>/\1/g')
    export TABLE_CLIENT_VERSION
    TABLE_COMMIT=$(git rev-parse HEAD)
    echo "table client version is $TABLE_CLIENT_VERSION, commit is $TABLE_COMMIT"
    {
      echo ""
      echo "[TABLE CLIENT]"
      echo "REVISION: $TABLE_COMMIT"
      echo "BUILD_BRANCH: $table_branch"
      echo "BUILD_VERSION: $TABLE_CLIENT_VERSION"
      echo "BUILD_URL: $table_url"
    } >> "$package_dir"/.BUILD_COMMITS
    mvn clean package install -Dmaven.test.skip=true -Dgpg.skip=true -Dcheckstyle.skip=true
}



# hbase client
function compileHBase(){
    cd "$package_dir"/temp
    count=100
    set +e
    while [ $count -gt 1 ]
    do
        count=$(( count - 1 ))
        rm -rf obkv-hbase-client-java
        git clone --depth 1 -b $hbase_branch $hbase_url
        if [ $? -eq 0 ]; then
            count=0
        fi
    done
    set -e
    cd obkv-hbase-client-java
    HBASE_CLIENT_VERSION=$(grep -o '<version>.*</version>' pom.xml | head -n1 | sed 's/<version>\(.*\)<\/version>/\1/g')
    HBASE_COMMIT=$(git rev-parse HEAD)
    echo "hbase client version is $HBASE_CLIENT_VERSION, commit is $HBASE_COMMIT"
    {
      echo ""
      echo "[HBASE CLIENT]"
      echo "REVISION: $HBASE_COMMIT"
      echo "BUILD_BRANCH: $hbase_branch"
      echo "BUILD_VERSION: $HBASE_CLIENT_VERSION"
      echo "BUILD_URL: $hbase_url"
    } >> "$package_dir"/.BUILD_COMMITS
    if [ "$hbase_use_local_table" = "true" ]; then
       mvn clean package install -Dmaven.test.skip=true -Dgpg.skip=true -Dcheckstyle.skip=true -Dtable.client.version="$TABLE_CLIENT_VERSION"
    else
      mvn clean package install -Dmaven.test.skip=true -Dgpg.skip=true -Dcheckstyle.skip=true
    fi
}

echo "hbase_use_local_table is $hbase_use_local_table"
cd "$package_dir" && rm -rf temp && mkdir temp
rm -rf "$package_dir"/.BUILD_COMMITS && touch "$package_dir"/.BUILD_COMMITS
compileTable
compileHBase
cd "$package_dir" && rm -rf temp
echo "mvn clean package install -Dmaven.test.skip=true -Dgpg.skip=true -Dcheckstyle.skip=true -Dtable.client.version=$TABLE_CLIENT_VERSION -Dhbase.client.version=$HBASE_CLIENT_VERSION"
mvn clean package install -Dmaven.test.skip=true -Dgpg.skip=true -Dcheckstyle.skip=true -Dtable.client.version="$TABLE_CLIENT_VERSION" -Dhbase.client.version="$HBASE_CLIENT_VERSION"


