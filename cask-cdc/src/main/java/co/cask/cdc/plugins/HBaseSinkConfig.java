/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdc.plugins;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.common.ReferencePluginConfig;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Plugin {@link Config} for Apache HBase.
 */
public class HBaseSinkConfig extends ReferencePluginConfig {

  @Name("zookeeperQuorum")
  @Nullable
  @Description("Zookeeper Quorum. By default it is set to 'localhost'")
  private String zkQuorum;

  @Name("zookeeperClientPort")
  @Nullable
  @Macro
  @Description("Zookeeper Client Port. By default it is set to 2181")
  private String zkClientPort;

  @Name("zookeeperParent")
  @Nullable
  @Macro
  @Description("Parent Node of HBase in Zookeeper. Default to '/hbase'")
  private String zkNodeParent;

  public HBaseSinkConfig(String referenceName) {
    super(referenceName);
  }

  @Nullable
  public String getZkQuorum() {
    return zkQuorum;
  }

  @Nullable
  public String getZkClientPort() {
    return zkClientPort;
  }

  @Nullable
  public String getZkNodeParent() {
    return zkNodeParent;
  }
}