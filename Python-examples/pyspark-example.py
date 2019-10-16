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

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('yarn').getOrCreate()

# load by specifying path to file in HDFS
flights = spark.read.parquet('/user/hive/warehouse/flights/')

# load by specifying name of table in metastore
flights = spark.table('flights')

from pyspark.sql.functions import col, lit, count, mean

# query using DataFrame API
flights \
  .filter(col('dest') == lit('LAS')) \
  .groupBy('origin') \
  .agg( \
       count('*').alias('num_departures'), \
       mean('dep_delay').alias('avg_dep_delay') \
  ) \
  .orderBy('avg_dep_delay') \
  .show()

# query using SQL
spark.sql('''
  SELECT origin,
    COUNT(*) AS num_departures,
    AVG(dep_delay) AS avg_dep_delay
  FROM flights
  WHERE dest = 'LAS'
  GROUP BY origin
  ORDER BY avg_dep_delay''').show()

spark.stop()
