#!/bin/bash

# Copyright 2019 Cloudera, Inc.
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

# Bash script to create and populate tables to demonstrate Python and R interfaces to Spark and Impala

# Change the values of these variables before running
DEFAULT_DATABASE_NAME="default"
DEFAULT_DATABASE_LOCATION="hdfs:///user/hive/warehouse"
IMPALAD_HOST_PORT="hostname:21000"


put_file_if_dir_is_empty(){ # args: $1 = directory of local file, $2 = file name, $3 HDFS directory
    dir_empty=$(hdfs dfs -count $3 | awk '{print $2}')
  if [[ $dir_empty -eq 0 ]]; then
    hdfs dfs -put $1/$2 $3/$2
  else
    echo ERROR: $3 is not empty
    exit 1
  fi
}

put_file_no_headers_if_dir_is_empty(){ # args: $1 = directory of local file, $2 = file name, $3 HDFS directory
    dir_empty=$(hdfs dfs -count $3 | awk '{print $2}')
  if [[ $dir_empty -eq 0 ]]; then
    tail -n +2 $1/$2 | hdfs dfs -put - $3/$2
  else
    echo ERROR: $3 is not empty
    exit 1
  fi
}


impala-shell -i $IMPALAD_HOST_PORT --var=dbname=$DEFAULT_DATABASE_NAME --var=location=$DEFAULT_DATABASE_LOCATION -q '
  CREATE DATABASE IF NOT EXISTS ${var:dbname} LOCATION "${var:location}";
' || { echo ERROR: Failed to create database $DEFAULT_DATABASE_NAME ; exit 1; }


impala-shell -i $IMPALAD_HOST_PORT --var=dbname=$DEFAULT_DATABASE_NAME --var=location=$DEFAULT_DATABASE_LOCATION -q '
  CREATE TABLE ${var:dbname}.flights
     (year SMALLINT,
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
    LOCATION "${var:location}/flights/";
' || { echo ERROR: Failed to create table flights in database $DEFAULT_DATABASE_NAME ; exit 1; }

put_file_if_dir_is_empty data/flights flights.parquet $DEFAULT_DATABASE_LOCATION/flights

impala-shell -i $IMPALAD_HOST_PORT --var=dbname=$DEFAULT_DATABASE_NAME --var=location=$DEFAULT_DATABASE_LOCATION -q '
  CREATE TABLE ${var:dbname}.airlines
     (carrier STRING,
      name STRING)
    ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ","
      LINES TERMINATED BY "\n" 
    STORED AS TEXTFILE
    LOCATION "${var:location}/airlines/";
' || { echo ERROR: Failed to create table airlines in database $DEFAULT_DATABASE_NAME ; exit 1; }

put_file_no_headers_if_dir_is_empty data/airlines airlines.csv $DEFAULT_DATABASE_LOCATION/airlines

impala-shell -i $IMPALAD_HOST_PORT --var=dbname=$DEFAULT_DATABASE_NAME -q '
  REFRESH ${var:dbname}.flights;
  REFRESH ${var:dbname}.airlines;
' || { echo ERROR: Failed to refresh tables in database $DEFAULT_DATABASE_NAME ; exit 1; }
