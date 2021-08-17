# Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

#!/bin/bash

# First delete the existing topic
/Users/rmahindra/Infinilake/kafka/bin/kafka-topics.sh --delete --topic hudi-test-topic --bootstrap-server localhost:9092

# Create the topic with 4 partitions
/Users/rmahindra/Infinilake/kafka/bin/kafka-topics.sh --create --topic hudi-test-topic --partitions 4 --replication-factor 1 --bootstrap-server localhost:9092

# Generate kafka messages
for ((cnt=0; cnt<=$1; cnt++ )); do cat raw.json |  jq --argjson cnt $cnt -c '.volume = $cnt' | kafkacat -P -b localhost:9092 -t hudi-test-topic; if [ $(( $cnt % 1000 )) -eq 0 ]; then sleep 1; fi; done;

