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

package org.apache.hudi.hive.util;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Utility class to sync a hudi table (meta) with the AWS Glue Metastore,
 * which enables querying the Hudi tables using AWS Athena, Glue ETLs.
 *
 * Extends HiveSyncTool since most logic is similar to Hive syncing,
 * expect using a different client {@link HoodieGlueClient} that implements
 * the necessary functionality using Glue APIs.
 */
public class GlueSyncTool extends HiveSyncTool {

  public GlueSyncTool(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    super(enableGlueSync(cfg), configuration, fs);
  }

  @Override
  public void syncHoodieTable() {
    super.syncHoodieTable();
  }

  private static HiveSyncConfig enableGlueSync(HiveSyncConfig cfg) {
    cfg.isAwsGlueMetaSyncEnabled = true;
    return cfg;
  }

  public static void main(String[] args) {
    // parse the params
    final HiveSyncConfig cfg = new HiveSyncConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    FileSystem fs = FSUtils.getFs(cfg.basePath, new Configuration());
    // HiveConf is unused, but passed to maintain constructor structure
    // as deltastreamer uses reflection.
    new GlueSyncTool(cfg, new HiveConf(), fs).syncHoodieTable();
  }
}
