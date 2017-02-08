/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.solr;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.solr.schema.SolrSchemaFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;


public class SolrStoragePlugin extends AbstractStoragePlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SolrStoragePlugin.class);
  private final SolrStoragePluginConfig solrStorageConfig;
  private final SolrClient solrClient;
  private final DrillbitContext context;
  private final SolrSchemaFactory schemaFactory;

  public SolrStoragePlugin(SolrStoragePluginConfig solrStoragePluginConfig,
      DrillbitContext context, String name) throws DrillbitStartupException {
    SolrStoragePlugin.logger.info("initializing solr storage plugin....");
    this.context = context;
    this.solrStorageConfig = solrStoragePluginConfig;
    this.solrClient = createSolrClient(solrStorageConfig.getSolrServer(),
        solrStoragePluginConfig.getSolrMode()); // create solrClient

    this.schemaFactory = createSchemaFactory(name);
  }

  protected SolrSchemaFactory createSchemaFactory(String storageName) {
    return new SolrSchemaFactory(this, storageName);
  }

  protected SolrClient createSolrClient(String solrServer, String solrMode) {
    SolrClient solrClient;

    switch (solrMode.toLowerCase()) {
    case "http":
      SolrStoragePlugin.logger.debug(" creating a http solr client...");
      solrClient = new HttpSolrClient(solrServer);

      break;

    default:
      SolrStoragePlugin.logger
      .debug(" creating a http solr client.solrMode is empty is storage config..");
      solrClient = new HttpSolrClient(solrServer);

      break;
    }

    return solrClient;
  }

  @Override
  public StoragePluginConfig getConfig() {
    return this.solrStorageConfig;
  }

  public DrillbitContext getContext() {
    return this.context;
  }

  @Override
  public Set<StoragePluginOptimizerRule> getOptimizerRules(
      OptimizerRulesContext optimizerRulesContext) {
    Set<StoragePluginOptimizerRule> storagePluginOptimizerRules = ImmutableSet.of(
        SolrQueryLimitRule.LIMIT_ON_SCAN, SolrQueryFilterRule.FILTER_ON_SCAN,
        SolrQueryLimitRule.LIMIT_ON_PROJECT);

    if(!solrStorageConfig.getSolrStorageProperties().isUseSolrStream()) {

      storagePluginOptimizerRules = ImmutableSet.of(SolrQueryFilterRule.FILTER_ON_SCAN, SolrQueryFilterRule.AGG_PUSH_DOWN,
          SolrQueryFilterRule.AGG_PUSH_DOWN_LIMIT, SolrQueryFilterRule.AGG_PUSH_DOWN_LIMIT2,
          SolrQueryFilterRule.AGG_FILTER_PUSH_DOWN3,
          SolrQueryFilterRule.PROJECT_AGG_PUSH_DOWN, SolrQueryFilterRule.AGG_FILTER_PUSH_DOWN,
          SolrQueryFilterRule.AGG_FILTER_PUSH_DOWN_LIMIT, SolrQueryFilterRule.AGG_FILTER_PUSH_DOWN2,
          SolrQueryLimitRule.LIMIT_ON_SCAN, SolrQueryLimitRule.LIMIT_ON_PROJECT,
          SolrQueryFilterRule.AGG_PUSH_DOWN_WITH_SORT_LIMIT,
          SolrQueryFilterRule.AGG_PUSH_DOWN_WITH_SORT);
    }
    return storagePluginOptimizerRules;
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection,
      List<SchemaPath> columns) throws IOException {
    SolrScanSpec solrScanSpec = selection.getListWith(new ObjectMapper(),
        new TypeReference<SolrScanSpec>() {
    });

    return new SolrGroupScan(userName, this, solrScanSpec, columns);
  }

  public SolrSchemaFactory getSchemaFactory() {
    return schemaFactory;
  }

  public SolrClient getSolrClient() {
    return this.solrClient;
  }

  public SolrStoragePluginConfig getSolrStorageConfig() {
    return solrStorageConfig;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public boolean supportsRead() {
    return true;
  }
}
