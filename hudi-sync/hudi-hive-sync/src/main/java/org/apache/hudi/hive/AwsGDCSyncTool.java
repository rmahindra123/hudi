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

package org.apache.hudi.hive;

import org.apache.hudi.common.fs.FSUtils;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Currently Experimental. Utility class that implements syncing a Hudi Table with the
 * AWS Glue Data Catalog (https://docs.aws.amazon.com/glue/latest/dg/populate-data-catalog.html)
 * to enable querying via Glue ETLs, Athena etc.
 *
 * Extends HiveSyncTool since most logic is similar to Hive syncing,
 * expect using a different client {@link GlueDataCatalogClient} that implements
 * the necessary functionality using Glue APIs.
 */
public class AwsGDCSyncTool extends HiveSyncTool {

  public AwsGDCSyncTool(HiveSyncConfig cfg, FileSystem fs) {
    super(enableGlueSync(cfg), new HiveConf(), fs);
  }
  
  private static HiveSyncConfig enableGlueSync(HiveSyncConfig hiveSyncConfig) {
    hiveSyncConfig.isAwsGlueDataCatalogSyncEnabled = true;
    return hiveSyncConfig;
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
    new AwsGDCSyncTool(cfg, fs).syncHoodieTable();
  }
}
