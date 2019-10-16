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
  raise Exception('These pandas examples require Python 3')
else:
  !pip3 install -U pandas

import pandas as pd

flights = pd.read_csv('data/flights.csv')

flights \
  .loc[flights.dest == "LAS", :] \
  .groupby('origin') \
  .agg(
    num_departures=('flight','count'), \
    avg_dep_delay=('dep_delay','mean'), \
  ) \
  .reset_index() \
  .sort_values('avg_dep_delay')
