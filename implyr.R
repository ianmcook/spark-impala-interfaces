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

# This script requires that the Impala ODBC driver is installed on the host
# Modify .odbc.ini and .odbcinst.ini before running

if(!"implyr" %in% rownames(installed.packages())) {
  install.packages("implyr")
}
if(!"odbc" %in% rownames(installed.packages())) {
  install.packages("odbc")
}

library(DBI)
library(implyr)

drv <- odbc::odbc()
impala <- src_impala(
    drv = drv,
    dsn = "Impala DSN"
  )

flights <- tbl(impala, "flights")

library(dplyr)

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
tbl(impala, sql("
  SELECT origin,
    COUNT(*) AS num_departures,
    AVG(dep_delay) AS avg_dep_delay
  FROM flights
  WHERE dest = 'LAS'
  GROUP BY origin
  ORDER BY avg_dep_delay"))

dbDisconnect(impala)
