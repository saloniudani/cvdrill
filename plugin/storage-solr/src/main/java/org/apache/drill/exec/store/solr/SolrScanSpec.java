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

import java.util.List;

import org.apache.drill.exec.store.solr.schema.SolrSchemaResp;
import org.apache.solr.client.solrj.response.QueryResponse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;


public class SolrScanSpec {
  private String solrCoreName;
  private boolean isAggregateQuery = false;
  private boolean isGroup = false;
  private boolean isDataQuery = false;
  private SolrFilterParam filter = new SolrFilterParam();
  private List<SolrAggrParam> aggrParams = Lists.newArrayList();
  private List<SolrSortParam> sortParams = Lists.newArrayList();
  private List<String> projectFieldNames = Lists.newArrayList();
  private long solrDocFetchCount = -1;
  private SolrSchemaResp cvSchema;
  private List<String> responseFieldList;
  private String solrUrl;
  private boolean isDistinct = false;
  private SolrClientAPIExec solrClientApiExec = new SolrClientAPIExec();
  private boolean isLimitApplied = false;

  @JsonCreator
  public SolrScanSpec(@JsonProperty("solrCoreName")
  String solrCoreName) {

    this.solrCoreName = solrCoreName;
  }

  @JsonCreator
  public SolrScanSpec(@JsonProperty("solrCoreName")
  String solrCoreName,
  @JsonProperty("solrDocFetchCount")
  long solrDocFetchCount) {
    this.solrCoreName = solrCoreName;
    this.solrDocFetchCount = solrDocFetchCount;
  }

  public SolrScanSpec(@JsonProperty("solrCoreName")
  String solrCoreName, @JsonProperty("filter")
  SolrFilterParam filter) {
    this.solrCoreName = solrCoreName;
    this.filter = filter;
  }

  @JsonCreator
  public SolrScanSpec(@JsonProperty("solrCoreName")
  String solrCoreName,
  @JsonProperty("solrCoreSchema")
  SolrSchemaResp cvSchema, @JsonProperty("solrUrl")
  String solrUrl) {
    this.solrCoreName = solrCoreName;
    this.cvSchema = cvSchema;
    this.solrUrl = solrUrl;
  }

  @JsonCreator
  public SolrScanSpec(@JsonProperty("solrCoreName")
  String solrCoreName, @JsonProperty("filter")
  String filter) {
    this.solrCoreName = solrCoreName;
  }



  public List<SolrAggrParam> getAggrParams() {
    return aggrParams;
  }

  public SolrSchemaResp getCvSchema() {
    return cvSchema;
  }

  public SolrFilterParam getFilter() {
    return filter;
  }

  public List<String> getProjectFieldNames() {
    return projectFieldNames;
  }

  public List<String> getResponseFieldList() {
    return responseFieldList;
  }

  public String getSolrCoreName() {
    return solrCoreName;
  }

  public long getSolrDocFetchCount(String solrCore, List<String> fields) {
    if (solrDocFetchCount == -1) {
      StringBuilder filterBuilder = new StringBuilder();
      if (filter != null) {
        for (String filter : filter) {
          filterBuilder.append(filter);
        }
      }
      QueryResponse rsp = solrClientApiExec.getNumFound(solrUrl, solrCore, filterBuilder,
          cvSchema.getUniqueKey(), fields);
      solrDocFetchCount = rsp != null ? rsp.getResults().getNumFound() : -1;

    }
    return solrDocFetchCount;
  }

  public String getSolrUrl() {
    return solrUrl;
  }

  public List<SolrSortParam> getSortParams() {
    return sortParams;
  }

  public boolean isAggregateQuery() {
    return isAggregateQuery;
  }

  public boolean isDataQuery() {
    return isDataQuery;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  public boolean isGroup() {
    return isGroup;
  }

  public boolean isLimitApplied() {
    return isLimitApplied;
  }

  public void setAggregateQuery(boolean isAggregateQuery) {
    this.isAggregateQuery = isAggregateQuery;
  }

  public void setAggrParams(List<SolrAggrParam> aggrParams) {
    this.aggrParams = aggrParams;
  }

  public void setCvSchema(SolrSchemaResp cvSchema) {
    this.cvSchema = cvSchema;
  }

  public void setDataQuery(boolean isDataQuery) {
    this.isDataQuery = isDataQuery;
  }

  public void setDistinct(boolean isDistinct) {
    this.isDistinct = isDistinct;
  }

  public void setFilter(SolrFilterParam filter) {
    this.filter = filter;
  }

  public void setGroup(boolean isGroup) {
    this.isGroup = isGroup;
  }

  public void setLimitApplied(boolean isLimitApplied) {
    this.isLimitApplied = isLimitApplied;
  }

  public void setProjectFieldNames(List<String> projectFieldNames) {
    this.projectFieldNames = projectFieldNames;
  }

  public void setResponseFieldList(List<String> responseFieldList) {
    this.responseFieldList = responseFieldList;
  }

  public void setSolrCoreName(String solrCoreName) {
    this.solrCoreName = solrCoreName;
  }

  public void setSolrDocFetchCount(long solrDocFetchCount) {
    this.solrDocFetchCount = solrDocFetchCount;
  }

  public void setSolrUrl(String solrUrl) {
    this.solrUrl = solrUrl;
  }

  public void setSortParams(List<SolrSortParam> sortParams) {
    this.sortParams = sortParams;
  }

  @Override
  public String toString() {
    return "SolrScanSpec [solrCoreName=" + solrCoreName + ", solrUrl=" +
        solrUrl + " filter=" + filter + ", solrDocFetchCount=" +
        solrDocFetchCount + " aggreegation=" + aggrParams + "]";
  }
}
