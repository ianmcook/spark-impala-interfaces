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

if sys.version_info[0] == 2:
  !pip install impyla
  !pip install thrift==0.9.3
else:
  !pip3 install impyla
  !pip3 install thrift

from impala.dbapi import connect
import pandas as pd

con = connect(host='hostname', port=21050)

sql = ''' 
  SELECT origin,
    COUNT(*) AS num_departures,
    AVG(dep_delay) AS avg_dep_delay
  FROM flights
  WHERE dest = 'LAS'
  GROUP BY origin
  ORDER BY avg_dep_delay'''

pd.read_sql(sql, con)
