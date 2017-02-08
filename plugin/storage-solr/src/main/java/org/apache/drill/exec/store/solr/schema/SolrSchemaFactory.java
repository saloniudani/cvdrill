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
package org.apache.drill.exec.store.solr.schema;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.solr.SolrClientAPIExec;
import org.apache.drill.exec.store.solr.SolrScanSpec;
import org.apache.drill.exec.store.solr.SolrStoragePlugin;
import org.apache.drill.exec.store.solr.SolrStoragePluginConfig;
import org.apache.drill.exec.store.solr.SolrStorageProperties;
import org.apache.drill.exec.store.solr.datatype.SolrDataType;
import org.apache.drill.exec.store.sys.StaticDrillTable;
import org.apache.http.client.ClientProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


public class SolrSchemaFactory implements SchemaFactory {
  class SolrSchema extends AbstractSchema {
    private Set<String> availableSolrCores = new HashSet<String>();
    private String currentSchema = "root";
    private final Map<String, DrillTable> drillTables = Maps.newHashMap();
    private final SolrStoragePlugin solrStoragePlugin;
    private final List<String> schemaPath;
    protected final SolrClientAPIExec solrClientApiExec;
    private boolean isAnonymous = false;
    private SchemaConfig schemaConfig;

    public SolrSchema(List<String> schemaPath, String currentSchema,
        SolrStoragePlugin solrStoragePlugin,SchemaConfig schemaConfig)
            throws Exception {
      super(schemaPath, currentSchema);
      this.schemaPath = schemaPath;
      this.currentSchema = currentSchema;
      this.solrStoragePlugin = solrStoragePlugin;
      this.solrClientApiExec = new SolrClientAPIExec(solrStoragePlugin.getSolrClient());
      this.schemaConfig = schemaConfig;

      String userName = getLoggedInUserName();
      isAnonymous = userName.equalsIgnoreCase("anonymous");
      availableSolrCores = this.solrClientApiExec.getSolrCoreList();

    }

    private void createOrReplaceViews() {
      SolrSchemaFactory.logger.trace(
          "SolrStoragePlugin :: createORReplaceViews");

      String solrServerUrl = this.solrStoragePlugin.getSolrStorageConfig()
          .getSolrServer();
      String solrCoreViewWorkspace = this.solrStoragePlugin.getSolrStorageConfig()
          .getSolrCoreViewWorkspace();
      String schemaUrl = this.solrStoragePlugin.getSolrStorageConfig()
          .getSolrStorageProperties()
          .getSolrSchemaUrl();

      try {
        if (!availableSolrCores.isEmpty()) {
          for (String coreName : availableSolrCores) {
            SolrSchemaResp oCVSchema = this.solrClientApiExec.getSchemaForCore(coreName,
                solrServerUrl, schemaUrl);
            this.solrClientApiExec.createSolrView(coreName,
                solrCoreViewWorkspace, oCVSchema);
          }
        } else {
          SolrSchemaFactory.logger.debug(
              "There is no cores in the current solr server : " +
                  solrServerUrl);
        }
      } catch (ClientProtocolException e) {
        SolrSchemaFactory.logger.debug("creating view failed : " +
            e.getMessage());
      } catch (IOException e) {
        SolrSchemaFactory.logger.debug("creating view failed : " +
            e.getMessage());
      }
    }


    DrillTable getDrillTable(String dbName, String dataSourceName) {
      SolrSchemaFactory.logger.debug("SolrSchema :: getDrillTable of " +
          dataSourceName);

      if (!drillTables.containsKey(dataSourceName)) { // table does not exist

        return null;
      }

      return drillTables.get(dataSourceName);
    }

    private String getLoggedInUserName() {
      String userName = schemaConfig.getUserName();
      return userName;
    }


    @Override
    public Table getTable(String coreName) {
      SolrSchemaFactory.logger.trace("SolrSchema :: getTable of " +
          coreName);

      SolrSchemaResp oCVSchema = null;
      DrillTable drillTable = null;

      // table does not exist
      if (!availableSolrCores.contains(coreName)) {
        return null;
      }

      if (!drillTables.containsKey(coreName)) {
        String solrServerUrl = this.solrStoragePlugin.getSolrStorageConfig()
            .getSolrServer();
        String schemaUrl = this.solrStoragePlugin.getSolrStorageConfig()
            .getSolrStorageProperties()
            .getSolrSchemaUrl();

        oCVSchema = this.solrClientApiExec.getSchemaForCore(coreName, solrServerUrl, schemaUrl);
        try {
          SolrScanSpec scanSpec = new SolrScanSpec(coreName,
              oCVSchema, solrServerUrl);
          drillTable = new StaticDrillTable(SolrStoragePluginConfig.NAME,
              solrStoragePlugin, scanSpec,
              new SolrDataType(scanSpec.getCvSchema()));

          drillTables.put(coreName, drillTable);
        } catch (Exception e) {
          SolrSchemaFactory.logger.error(
              "Unable to parse solr schema..." + e.getMessage());
        }
      } else {
        drillTable = drillTables.get(coreName);
      }

      return drillTable;
    }

    @Override
    public Set<String> getTableNames() {
      SolrStorageProperties solrStorageConfig = this.solrStoragePlugin.getSolrStorageConfig()
          .getSolrStorageProperties();

      if (solrStorageConfig.isCreateViews()) {
        createOrReplaceViews();
      }

      return availableSolrCores;
    }

    @Override
    public String getTypeName() {
      return SolrStoragePluginConfig.NAME;
    }

    void setHolder(SchemaPlus plusOfThis) {
      for (String solrCore : availableSolrCores) {
        plusOfThis.add("root", getSubSchema(solrCore));
      }
    }

    @Override
    public boolean showInInformationSchema() {
      return true;
    }
  }
  static final Logger logger = LoggerFactory.getLogger(SolrSchemaFactory.class);
  private final SolrStoragePlugin solrStorage;

  private final String storageName;

  public SolrSchemaFactory(SolrStoragePlugin solrStorage, String storageName) {
    this.solrStorage = solrStorage;
    this.storageName = storageName;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.drill.exec.store.SchemaFactory#registerSchemas(org.apache.drill .exec.store.SchemaConfig,
   * org.apache.calcite.schema.SchemaPlus)
   */
  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent)
      throws IOException {
    List<String> schemaPath = Lists.newArrayList();
    schemaPath.add(SolrStoragePluginConfig.NAME);

    SolrSchema schema;

    try {
      schema = new SolrSchema(schemaPath, "root", solrStorage,
          schemaConfig);

      SchemaPlus hPlus = parent.add(this.storageName, schema);
    } catch (Exception e) {
      SolrSchemaFactory.logger.error(e.getMessage());
    }
  }
}
