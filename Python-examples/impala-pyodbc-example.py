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

# This script requires that the Impala ODBC driver is installed on the host
# Modify .odbc.ini and .odbcinst.ini before running

if sys.version_info[0] == 2:
  !pip install pyodbc
else:
  !pip3 install pyodbc

import pyodbc
import pandas as pd

con = pyodbc.connect('DSN=Impala DSN', autocommit=True)

sql = ''' 
  SELECT origin,
    COUNT(*) AS num_departures,
    AVG(dep_delay) AS avg_dep_delay
  FROM flights
  WHERE dest = 'LAS'
  GROUP BY origin
  ORDER BY avg_dep_delay'''

pd.read_sql(sql, con)
