#!/bin/bash

# Copyright 2018 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Replace hostname with an impalad hostname before running

impala-shell -i hostname:21000 -q '
  CREATE TABLE default.airlines (
    `carrier` STRING,
    `name` STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
  LOCATION "hdfs:///user/hive/warehouse/airlines/";'

airlines_empty=$(hdfs dfs -count /user/hive/warehouse/airlines/ | awk '{print $2}')
if [[ $airlines_empty -eq 0 ]]; then
  hdfs dfs -put data/airlines.csv /user/hive/warehouse/airlines/
else
  echo ERROR: /user/hive/warehouse/airlines/ is not empty
  exit 1
fi

impala-shell -i hostname:21000 -q '
  CREATE TABLE default.flights (
      year SMALLINT,
      month TINYINT,
      day TINYINT,
      dep_time SMALLINT,
      sched_dep_time SMALLINT,
      dep_delay SMALLINT,
      arr_time SMALLINT,
      sched_arr_time SMALLINT,
      arr_delay SMALLINT,
      carrier STRING,
      flight SMALLINT,
      tailnum STRING,
      origin STRING,
      dest STRING,
      air_time SMALLINT,
      distance SMALLINT,
      hour TINYINT,
      minute TINYINT,
      time_hour TIMESTAMP)
    STORED AS PARQUET
    LOCATION "hdfs:///user/hive/warehouse/flights/";'

flights_empty=$(hdfs dfs -count /user/hive/warehouse/flights/ | awk '{print $2}')
if [[ $flights_empty -eq 0 ]]; then
  hdfs dfs -put data/flights.parquet /user/hive/warehouse/flights/
else
  echo ERROR: /user/hive/warehouse/flights/ is not empty
  exit 1
fi
