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

flights <- read.csv("data/flights.csv")

flights_to_las <- flights[flights$dest == "LAS", ]
flights_to_las_agg <- cbind(
	setNames(aggregate(dest ~ origin, data = flights_to_las, FUN = length), c("origin", "num_departures")),
	avg_dep_delay = aggregate(dep_delay ~ origin, data = flights_to_las, FUN = mean, na.rm = TRUE)$dep_delay
)
flights_to_las_agg[order(flights_to_las_agg$avg_dep_delay), ]
