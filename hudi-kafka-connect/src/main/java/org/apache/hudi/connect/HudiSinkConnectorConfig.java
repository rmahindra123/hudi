/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public class HudiSinkConnectorConfig extends AbstractConfig {

  public static final String LEADER_COMMIT_INTERVAL_SEC_CONFIG = "leader.commit.interval";
  private static final String LEADER_COMMIT_INTERVAL_DOC = "The interval in secs for each hudi commit";
  private static final int LEADER_COMMIT_INTERVAL_SEC_DEFAULT = 10;

  public static final String LEADER_COMMIT_TIMEOUT_SEC_CONFIG = "second.required.param";
  private static final String LEADER_COMMIT_TIMEOUT_DOC = "The minimum timeout after sending an end commit that the leader should ignore the commit and start a new one";
  private static final int LEADER_COMMIT_TIMEOUT_SEC_DEFAULT = 5;

  public HudiSinkConnectorConfig(Map<String, String> props) {
    super(createConfigDef(), props);
  }

  public String getName() {
    return "HudiSinkConnector";
  }

  public static ConfigDef getConfig() {
    return createConfigDef();
  }

  private static ConfigDef createConfigDef() {
    ConfigDef configDef = new ConfigDef();
    addParams(configDef);
    return configDef;
  }

  private static void addParams(final ConfigDef configDef) {
    configDef
        .define(
            LEADER_COMMIT_INTERVAL_SEC_CONFIG,
            Type.INT,
            LEADER_COMMIT_INTERVAL_SEC_DEFAULT,
            Importance.HIGH,
            LEADER_COMMIT_INTERVAL_DOC)
        .define(
            LEADER_COMMIT_TIMEOUT_SEC_CONFIG,
            Type.INT,
            LEADER_COMMIT_TIMEOUT_SEC_DEFAULT,
            Importance.HIGH,
            LEADER_COMMIT_TIMEOUT_DOC);
  }
}
