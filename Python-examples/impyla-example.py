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

if sys.version_info[0] == 2:
  !pip install impyla
  !pip install thrift==0.9.3
  !pip install thrift_sasl
else:
  !pip3 install impyla==0.15.0
  !pip3 install sasl==0.2.1
  !pip3 install thrift_sasl==0.2.1
  !pip3 install thriftpy==0.3.9
  !pip3 install thriftpy2==0.4.0

from impala.dbapi import connect
import pandas as pd

con = connect(\
  host='cdsw-demo-2', \
  port=21050, \
  auth_mechanism='GSSAPI', \
  kerberos_service_name='impala' \
)

sql = ''' 
  SELECT origin,
    COUNT(*) AS num_departures,
    AVG(dep_delay) AS avg_dep_delay
  FROM flights
  WHERE dest = 'LAS'
  GROUP BY origin
  ORDER BY avg_dep_delay'''

pd.read_sql(sql, con)
