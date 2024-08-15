#!/bin/bash
#
# Copyright 2021-2024 EMQ Technologies Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script accepts the following parameters:
#
# * with_edgex
#
# Example:
#
# ./test/run_jmeter.sh with_edgex=true
#
# or
#
# ./test/run_jmeter.sh with_edgex=false
#

set -e

CONFIG=$@

for line in $CONFIG; do
  eval "$line"
done

function downloadjar
{
  if [ ! -f $1 ];then
    wget -O $1 $2
  else
    echo "Already downloaded $1."
  fi
}

downloadjar "/opt/jmeter/lib/json-lib-2.4-jdk15.jar" https://repo1.maven.org/maven2/net/sf/json-lib/json-lib/2.4/json-lib-2.4-jdk15.jar
downloadjar "/opt/jmeter/lib/commons-beanutils-1.8.0.jar" https://repo1.maven.org/maven2/commons-beanutils/commons-beanutils/1.8.0/commons-beanutils-1.8.0.jar
downloadjar "/opt/jmeter/lib/commons-collections-3.2.1.jar" https://repo1.maven.org/maven2/commons-collections/commons-collections/3.2.1/commons-collections-3.2.1.jar
downloadjar "/opt/jmeter/lib/commons-lang-2.5.jar" https://repo1.maven.org/maven2/commons-lang/commons-lang/2.5/commons-lang-2.5.jar
downloadjar "/opt/jmeter/lib/commons-logging-1.1.1.jar" https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar
downloadjar "/opt/jmeter/lib/ezmorph-1.0.6.jar" https://repo1.maven.org/maven2/net/sf/ezmorph/ezmorph/1.0.6/ezmorph-1.0.6.jar

ver=`git describe --tags --always`
os=`uname -s | tr "[A-Z]" "[a-z]"`
base_dir=_build/kuiper-"$ver"-"$os"-amd64
fvt_dir=`pwd`

rm -rf jmeter_logs

# Run mock nano lookup server
cd $base_dir
touch log/lookup_server.out
cd ../../test/mock/nano_lookup
export BUILD_ID=dontKillMe
echo "starting mock nano lookup..."
nohup ./server > ../../../$base_dir/log/lookup_server.out 2>&1 &
cd ../../../

echo -e "-------------------- canjson test ------------------------\n"
/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/can_mqttcan.jmx -Dfvt="$fvt_dir" -l jmeter_logs/can_mqttcan.jtl -j jmeter_logs/can_mqttcan.log
echo -e "---------------------------------------------\n"

#echo -e "-------------------- can test ------------------------\n"
#/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/can_socketcan.jmx -Dfvt="$fvt_dir" -l jmeter_logs/can_socketcan.jtl -j jmeter_logs/can_socketcan.log
#echo -e "---------------------------------------------\n"

echo -e "-------------------- nng lookup test ------------------------\n"
/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/nng_lookup.jmx -Dfvt="$fvt_dir" -l jmeter_logs/nng_lookup.jtl -j jmeter_logs/nng_lookup.log
echo -e "---------------------------------------------\n"

### Gee scenarios test

echo -e "-------------------- nng lookup test ------------------------\n"
/opt/jmeter/bin/jmeter.sh -Jjmeter.save.saveservice.output_format=xml -n -t test/nng_lookup.jmx -Dfvt="$fvt_dir" -l jmeter_logs/nng_lookup.jtl -j jmeter_logs/nng_lookup.log
echo -e "---------------------------------------------\n"