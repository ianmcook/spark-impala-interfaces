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

# Replace hdfs-hostname with an HDFS NameNode hostname before running

# Replace impala-hostname with an impalad hostname before running

if sys.version_info[0] == 2:
  !pip install impyla
  !pip install thrift_sasl
  !pip install ibis-framework[impala]
else:
  !pip3 install impyla==0.15.0
  !pip3 install sasl==0.2.1
  !pip3 install thrift_sasl==0.2.1
  !pip3 install thriftpy==0.3.9
  !pip3 install thriftpy2==0.4.0
  !pip3 install ibis-framework[impala]

import ibis
ibis.options.interactive = True

hdfs = ibis.hdfs_connect( \
  host='hdfs-hostname', \
  port=50070 \
)
con = ibis.impala.connect( \
  host='impala-hostname', \
  port=21050, \
  hdfs_client=hdfs, \
  auth_mechanism='GSSAPI', \
  kerberos_service_name='impala' \
)

flights = con.table('flights')

flights \
  .filter(flights.dest == 'LAS') \
  .group_by('origin') \
  .aggregate( \
     num_departures=flights.count(), \
     avg_dep_delay=flights.dep_delay.mean() \
  ) \
  .sort_by('avg_dep_delay') \
  .execute()
