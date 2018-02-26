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

if(!"sparklyr" %in% rownames(installed.packages())) {
  install.packages("sparklyr")
}

library(sparklyr)
library(dplyr)

spark <- spark_connect(master = "yarn")

# load by specifying path to file in HDFS
flights <- spark_read_parquet(
  sc = spark,
  name = "flights",
  path = "/user/hive/warehouse/flights/"
)

# load by specifying name of table in metastore
flights <- tbl(spark, "flights")

# query using dplyr
flights %>%
  filter(dest == "LAS") %>%
  group_by(origin) %>%
  summarise(
    num_departures = n(),
    avg_dep_delay = mean(dep_delay, na.rm = TRUE)
  ) %>%
  arrange(avg_dep_delay)

# query using SQL
tbl(spark, sql("
  SELECT origin,
    COUNT(*) AS num_departures,
    AVG(dep_delay) AS avg_dep_delay
  FROM flights
  WHERE dest = 'LAS'
  GROUP BY origin
  ORDER BY avg_dep_delay"))

spark_disconnect(spark)
