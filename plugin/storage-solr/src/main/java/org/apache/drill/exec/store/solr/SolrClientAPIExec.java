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
import java.io.InputStream;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.drill.exec.store.solr.schema.SolrSchemaField;
import org.apache.drill.exec.store.solr.schema.SolrSchemaResp;
import org.apache.drill.exec.store.solr.schema.SolrSchemaWrapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.CursorMarkParams;
import org.xml.sax.InputSource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class SolrClientAPIExec {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SolrClientAPIExec.class);
  private static final String solrSpecVersion = "response/lst[@name='lucene']/str[@name='solr-spec-version']";
  private SolrClient solrClient;

  public SolrClientAPIExec() {

  }

  public SolrClientAPIExec(SolrClient solrClient) {
    this.solrClient = solrClient;
  }

  /*
   * Can this be moved to client side option !!?
   */
  public void createSolrView(String solrCoreName, String solrCoreViewWorkspace,
      SolrSchemaResp oCVSchema) throws ClientProtocolException, IOException {

    List<SolrSchemaField> schemaFieldList = oCVSchema.getSchemaFields();
    List<String> fieldNames = Lists.newArrayList();
    String createViewSql = "CREATE OR REPLACE VIEW {0}.{1} as SELECT {2} from solr.{3}";
    for (SolrSchemaField cvSchemaField : schemaFieldList) {
      fieldNames.add("`" + cvSchemaField.getFieldName() + "`");
    }
    if (!fieldNames.isEmpty()) {
      String fieldStr = Joiner.on(",").join(fieldNames);
      int lastIdxOf = solrCoreName.lastIndexOf("_");
      String viewName = solrCoreName.toLowerCase() + "view";
      if (lastIdxOf > -1) {
        viewName = solrCoreName.substring(0, lastIdxOf).toLowerCase().replaceAll("_", "");
      }

      createViewSql = MessageFormat.format(createViewSql, solrCoreViewWorkspace, viewName,
          fieldStr, solrCoreName);
      SolrClientAPIExec.logger.debug("create solr view with sql command :: " + createViewSql);
      String drillWebUI = "http://localhost:8047/query";
      HttpClient client = HttpClientBuilder.create().build();
      HttpPost httpPost = new HttpPost(drillWebUI);
      List<BasicNameValuePair> urlParameters = new ArrayList<BasicNameValuePair>();
      urlParameters.add(new BasicNameValuePair("queryType", "SQL"));
      urlParameters.add(new BasicNameValuePair("query", createViewSql));
      httpPost.setEntity(new UrlEncodedFormEntity(urlParameters));
      HttpResponse response = client.execute(httpPost);
      SolrClientAPIExec.logger.debug("Response Code after executing create view command : "
          + response.getStatusLine().getStatusCode());
    } else {
      SolrClientAPIExec.logger
      .debug("No DataSource specific fields are found. Not going create a view for solr core [ "
          + solrCoreName + " ]");
    }

  }

  public QueryResponse getNumFound(String solrServer, String solrCoreName, StringBuilder filters,
      String uniqueKey, List<String> responseFieldList) {
    String solrUrl = solrServer;
    if (solrCoreName != null) {
      solrUrl = solrServer + solrCoreName;
    }
    SolrClient solrClient = new HttpSolrClient(solrUrl);
    SolrQuery solrQuery = new SolrQuery().setQuery("*:*").setStart(0)
        .setRows(0).addSort(SolrQuery.SortClause.desc(uniqueKey));

    if (filters != null && filters.length() > 0) {
      solrQuery.setParam("fq", filters.toString());
      SolrClientAPIExec.logger.debug("Filter query [ " + filters.toString() + " ]");
    }
    if (responseFieldList != null) {
      if (!responseFieldList.contains(uniqueKey)) {
        responseFieldList.add(uniqueKey);
      }
    } else {
      responseFieldList = Lists.newArrayListWithCapacity(1);
      responseFieldList.add(uniqueKey);
    }
    solrQuery.setParam("fl", Joiner.on(",").join(responseFieldList));
    QueryResponse rsp = null;
    try {
      SolrClientAPIExec.logger.info(
          "Submitting Solr warming up query :" + solrUrl + "/select"
              + solrQuery.toQueryString());

      rsp = solrClient.query(solrQuery);
      solrClient.close();

      SolrClientAPIExec.logger.info("Response recieved from [ " + solrServer + " ] in "
          + rsp.getQTime() + "ms.");
      SolrClientAPIExec.logger.info("Number of documents found :: "
          + rsp.getResults().getNumFound());
    } catch (SolrServerException | IOException e) {
      SolrClientAPIExec.logger.error("Unable to determine number of documents in the solr core :: "
          + e.getMessage());
    }

    return rsp;
  }

  public SolrSchemaResp getSchemaForCore(String coreName, String solrServerUrl, String schemaUrl) {

    schemaUrl = MessageFormat.format(schemaUrl, solrServerUrl, coreName);
    SolrClientAPIExec.logger.info("Getting schema information from :: " + schemaUrl);
    HttpClient client = HttpClientBuilder.create().build();
    HttpGet request = new HttpGet(schemaUrl);
    SolrSchemaResp oCVSchema = null;

    request.setHeader("Content-Type", "application/json");
    try {
      HttpResponse response = client.execute(request);
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      int respCode = response.getStatusLine().getStatusCode();
      SolrClientAPIExec.logger.debug("Response code from schema api :" + respCode);
      if (respCode >= 200 && respCode < 300) {
        SolrClientAPIExec.logger.trace("A Valid response returned.");
        Preconditions.checkNotNull(response.getEntity(),
            "Http Response Entity is invalid. Cannot proceed.");
        Preconditions.checkNotNull(response.getEntity().getContent(),
            "Http Entity Content is invalid. Cannot proceed.");
        SolrSchemaWrapper solrSchemaWrapper = mapper.readValue(response.getEntity().getContent(),
            SolrSchemaWrapper.class);
        oCVSchema = solrSchemaWrapper.getSchema();
      } else {
        SolrClientAPIExec.logger
        .error("Invalid response recieved while getting the schema details.");
      }
    } catch (Exception e) {
      SolrClientAPIExec.logger.info("Exception occured while fetching schema details..."
          + e.getMessage());
    }
    return oCVSchema;

  }

  public QueryResponse getSolr4Docs(String solrServer, String solrCoreName, String uniqueKey,
      List<String> fields, Long solrDocFectCount, String cursorMark, StringBuilder filters,
      List<SolrAggrParam> solrAggrParams, List<SolrSortParam> solrSortParams,
      List<String> aggrFieldNames, boolean isGroup, boolean useFacetPivotFromGroupCount) {

    String solrUrl = solrServer;
    if (solrCoreName != null) {
      solrUrl = solrServer + solrCoreName;
    }
    SolrClient solrClient = new HttpSolrClient(solrUrl);
    String fieldStr = null;
    String[] fieldListArr = null;
    List<String> statsFieldList = Lists.newArrayList();

    SolrQuery solrQuery = new SolrQuery().setTermsRegexFlag("case_insensitive")
        .setQuery(uniqueKey + ":*").setRows(0);

    if (filters.length() > 0) {
      solrQuery.setParam("fq", filters.toString());
      SolrClientAPIExec.logger.debug("Filter query [ " + filters.toString() + " ]");
    }

    if (!fields.isEmpty()) {
      fieldStr = Joiner.on(",").join(fields);
      solrQuery.setParam("fl", fieldStr);
      solrQuery.setRows(solrDocFectCount.intValue());
      SolrClientAPIExec.logger.debug("Response field list [" + fieldStr + "]");
    }
    // facet.pivot={!stats=s1}category,manufacturer
    // stats.field={!key=avg_price tag=s1 mean=true}price
    // stats.field={!tag=s1 min=true max=true}user_rating

    if (solrAggrParams != null && !solrAggrParams.isEmpty() && !useFacetPivotFromGroupCount) {
      solrQuery.setGetFieldStatistics(true);
      for (SolrAggrParam solrAggrParam : solrAggrParams) {
        String statsField = solrAggrParam.getFieldName();
        if (!fields.contains(statsField)) {
          statsField = uniqueKey;
        }
        if (!statsFieldList.contains(statsField)) {
          statsFieldList.add(statsField);
        }
        fields.remove(statsField);
      }
      if (!fields.isEmpty()) {
        fieldListArr = fields.toArray(new String[fields.size()]);
      }

      for (String statsField : statsFieldList) {
        solrQuery.setGetFieldStatistics(statsField);
        SolrClientAPIExec.logger.debug("Adding stats field parameter.. [ " + statsField + " ]");
        if (isGroup) {
          List<String> groupFields = Lists.newArrayList();
          for (String aggrField : aggrFieldNames) {
            if (fields.contains(aggrField)) {
              groupFields.add(aggrField);
            }
          }
          SolrClientAPIExec.logger.debug("Adding stats facet parameters.. [ " + groupFields + " ]");
          solrQuery.addStatsFieldFacets(statsField,
              groupFields.toArray(new String[groupFields.size()]));
        }

      }
      solrQuery.setRows(0);
    } else if (isGroup) {
      fieldListArr = fields.toArray(new String[fields.size()]);
      solrQuery.setFacet(true);
      if (fields.size() == 1) {
        solrQuery.addFacetField(fieldListArr);
        solrQuery.setFacetLimit(-1);
      } else {
        solrQuery.addFacetPivotField(Joiner.on(",").join(fields));
      }
      solrQuery.setRows(0);
      solrQuery.setFacetMinCount(1);

      // solrQuery.set(GroupParams.GROUP, true);
      // solrQuery.set(GroupParams.GROUP_FIELD, fieldListArr);
      // solrQuery.set(GroupParams.GROUP_MAIN, true);
      // solrQuery.set(GroupParams.GROUP_FORMAT, "simple");
      // solrQuery.set("group.ngroups", "true");
    }
    if (!solrSortParams.isEmpty()) {
      for (SolrSortParam solrSortParam : solrSortParams) {
        String solrSortField = solrSortParam.getSortFieldName();
        ORDER solrSortDir = solrSortParam.getSortDir();
        solrQuery.addSort(solrSortField, solrSortDir);

      }
    } else {
      solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
      solrQuery.setSort(SolrQuery.SortClause.desc(uniqueKey));
    }
    QueryResponse rsp = null;
    try {
      SolrClientAPIExec.logger.info("Submitting Query :" + solrServer + "/select"
          + solrQuery.toQueryString());

      rsp = solrClient.query(solrQuery);

      SolrClientAPIExec.logger.info("Response recieved from [ " + solrServer + " ] core [ "
          + solrCoreName + " ] in " + rsp.getQTime() + " MS.");

    } catch (SolrServerException | IOException e) {
      SolrClientAPIExec.logger.debug("Error occured while fetching results from solr server "
          + e.getMessage());
    } finally {
      try {
        solrClient.close();
      } catch (IOException e) {
        SolrClientAPIExec.logger.debug("Error occured while closing connection of solr server "
            + e.getMessage());
      }
    }

    return rsp;
  }

  public SolrClient getSolrClient() {
    return solrClient;
  }

  public Set<String> getSolrCoreList() {
    // Request core list
    SolrClientAPIExec.logger.debug("Getting cores from solr..");
    CoreAdminRequest request = new CoreAdminRequest();
    request.setAction(CoreAdminAction.STATUS);
    Set<String> coreList = null;
    try {
      CoreAdminResponse cores = request.process(solrClient);
      coreList = new HashSet<String>(cores.getCoreStatus().size());
      for (int i = 0; i < cores.getCoreStatus().size(); i++) {
        String coreName = cores.getCoreStatus().getName(i);
        coreList.add(coreName);
      }
    } catch (SolrServerException | IOException e) {
      SolrClientAPIExec.logger.info("Error getting core info from solr server...");
    }
    return coreList;
  }

  public QueryResponse getSolrDocs(String solrServer, String solrCoreName, String uniqueKey,
      List<String> fields, Long solrDocFectCount, String cursorMark, StringBuilder filters,
      List<SolrAggrParam> solrAggrParams, List<SolrSortParam> solrSortParams,
      List<String> aggrFieldNames, boolean isGroup, boolean isCountOnlyQuery) {

    String solrUrl = solrServer;
    if (solrCoreName != null) {
      solrUrl = solrServer + solrCoreName;
    }
    SolrClient solrClient = new HttpSolrClient(solrUrl);

    String fieldStr = null;
    String[] fieldListArr = null;
    List<String> statsFieldList = Lists.newArrayList();

    SolrQuery solrQuery = new SolrQuery().setQuery("{!cache=false}" + uniqueKey + ":*").setRows(0);

    if (filters.length() > 0) {
      solrQuery.setParam("fq", filters.toString());
      SolrClientAPIExec.logger.debug("Filter query [ " + filters.toString() + " ]");
    }

    if (!fields.isEmpty()) {
      fieldStr = Joiner.on(",").join(fields);
      solrQuery.setParam("fl", fieldStr);
      solrQuery.setRows(solrDocFectCount.intValue());
      SolrClientAPIExec.logger.debug("Response field list [" + fieldStr + "]");
    }
    if (solrAggrParams != null && !solrAggrParams.isEmpty() && !isCountOnlyQuery) {
      solrQuery.setGetFieldStatistics(true);
      String referenceToStatsTag = "{!stats=t1}";
      String statsTag = "{!tag=t1}";
      for (SolrAggrParam solrAggrParam : solrAggrParams) {
        String statsField = solrAggrParam.getFieldName();
        if (!fields.contains(statsField)) {
          statsField = uniqueKey;
        }
        if (!statsFieldList.contains(statsField)) {
          statsFieldList.add(statsField);
        }
        fields.remove(statsField);
      }
      if (!fields.isEmpty()) {
        fieldListArr = fields.toArray(new String[fields.size()]);
      }
      SolrClientAPIExec.logger.debug("Adding stats field parameter.." + statsFieldList + "");

      if (isGroup) {
        List<String> groupFields = Lists.newArrayList();
        solrQuery.addGetFieldStatistics(statsTag + Joiner.on(",").join(statsFieldList));
        for (String aggrField : fields) {
          if (fields.contains(aggrField)) {
            groupFields.add(aggrField);
          }
        }
        if (groupFields.size() == 1) {
          SolrClientAPIExec.logger.debug("Adding stats facet parameters.." + groupFields + "");
          for (String statsField : statsFieldList) {
            solrQuery.addStatsFieldFacets(statsField,
                groupFields.toArray(new String[groupFields.size()]));
          }
        } else {
          SolrClientAPIExec.logger.debug("Adding facet pivot parameters.." + groupFields + "");
          solrQuery.addFacetPivotField(referenceToStatsTag + Joiner.on(",").join(groupFields));
          solrQuery.setFacetLimit(-1);
        }
      } else {
        for (String statsField : statsFieldList) {
          solrQuery.setGetFieldStatistics(statsField);
        }
      }
      solrQuery.setRows(0);
    } else if (isGroup) {
      fieldListArr = fields.toArray(new String[fields.size()]);
      solrQuery.setFacet(true);
      if (fields.size() == 1) {
        solrQuery.addFacetField(fieldListArr);
        solrQuery.setFacetLimit(-1);
      } else {
        solrQuery.addFacetPivotField(Joiner.on(",").join(fields));
      }
      solrQuery.setRows(0);
      solrQuery.setFacetMinCount(1);
      solrQuery.setFacetLimit(-1);
    } else {
      solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
      solrQuery.addSort(SolrQuery.SortClause.desc(uniqueKey));

    }
    if (!solrSortParams.isEmpty()) {
      for (SolrSortParam solrSortParam : solrSortParams) {
        String solrSortField = solrSortParam.getSortFieldName();
        ORDER solrSortDir = solrSortParam.getSortDir();
        solrQuery.addSort(solrSortField, solrSortDir);

      }
    }

    QueryResponse rsp = null;
    try {
      SolrClientAPIExec.logger
      .info("Submitting Query :" + solrUrl + "/select"
          + solrQuery.toQueryString());

      rsp = solrClient.query(solrQuery);

      SolrClientAPIExec.logger.info("Response recieved from [ " + solrServer + " ] core [ "
          + solrCoreName + " ] in " + rsp.getQTime() + " MS.");

    } catch (SolrServerException | IOException e) {
      SolrClientAPIExec.logger.debug("Error occured while fetching results from solr server "
          + e.getMessage());
    } finally {
      try {
        solrClient.close();
      } catch (IOException e) {
        SolrClientAPIExec.logger.debug("Error occured while closing connection of solr server "
            + e.getMessage());
      }
    }

    return rsp;
  }

  public QueryResponse getSolrFieldStats(String solrServer, String solrCoreName, String uniqueKey,
      List<String> fields, StringBuilder filters) {

    solrCoreName = solrCoreName.replaceAll("`", "");
    SolrClient solrClient = new HttpSolrClient(solrServer + solrCoreName);

    SolrQuery solrQuery = new SolrQuery().setTermsRegexFlag("case_insensitive")
        .setQuery(uniqueKey + ":*").setRows(0);

    solrQuery.setGetFieldStatistics(true);
    for (String field : fields) {
      solrQuery.setGetFieldStatistics(field);
    }
    if (filters.length() > 0) {
      solrQuery.setParam("fq", filters.toString());
      SolrClientAPIExec.logger.info("filter query [ " + filters.toString() + " ]");
    }
    SolrClientAPIExec.logger
    .info("Setting up solrquery. Query String " + solrQuery.toQueryString());
    try {
      QueryResponse rsp = solrClient.query(solrQuery);
      SolrClientAPIExec.logger.info("Response recieved from [ " + solrServer + " ] core [ "
          + solrCoreName + " ]");
      return rsp;
    } catch (SolrServerException | IOException e) {
      SolrClientAPIExec.logger.debug("Error occured while fetching results from solr server "
          + e.getMessage());
    } finally {
      try {
        solrClient.close();
      } catch (IOException e) {

      }
    }
    return null;
  }

  public SolrStream getSolrStreamResponse(String solrServer, String solrCoreName,
      List<String> fields, StringBuilder filters, String uniqueKey, long solrDocFetchCount) {

    Map<String, String> solrParams = new HashMap<String, String>();
    solrParams.put("q", uniqueKey + ":*");

    solrParams.put("sort", uniqueKey + " desc ");
    solrParams.put("fl", Joiner.on(",").join(fields));
    solrParams.put("qt", "/export");
    if (solrDocFetchCount >= 0) {
      solrParams.put("rows", String.valueOf(solrDocFetchCount));
    }
    if (filters.length() > 0) {
      solrParams.put("fq", filters.toString());
      SolrClientAPIExec.logger.info("Filter query [ " + filters.toString() + " ]");
    }

    if (solrCoreName != null) {
      solrServer = solrServer + solrCoreName;
    }
    SolrClientAPIExec.logger.info("Sending request to solr server " + solrServer);
    SolrClientAPIExec.logger.info("Response field list " + fields);
    SolrStream solrStream = new SolrStream(solrServer, solrParams);
    return solrStream;

  }

  public void getSolrVersion() {

  }

  public String getSolrVersion(String solrServer) throws ClientProtocolException, IOException,
  XPathExpressionException {

    URL url = new URL(solrServer);
    String solrUrl = url.getProtocol() + "://" + url.getAuthority() + "/solr";
    String solrAdminUrl = solrUrl + "/admin/info/system?wt=xml";
    SolrClientAPIExec.logger.debug("Solr Admin Url is:" + solrAdminUrl);

    HttpClient client = HttpClientBuilder.create().build();
    HttpGet httpGet = new HttpGet(solrAdminUrl);

    HttpResponse response = client.execute(httpGet);
    int respCode = response.getStatusLine().getStatusCode();
    if (respCode >= 200 && respCode < 300) {
      InputStream is = response.getEntity().getContent();
      InputSource inputSource = new InputSource(is);

      XPath xPath = XPathFactory.newInstance().newXPath();
      String solrVersion = (String) xPath.evaluate(SolrClientAPIExec.solrSpecVersion, inputSource,
          XPathConstants.STRING);
      SolrClientAPIExec.logger.debug("Solr Version is" + solrVersion);

      return solrVersion;
    }
    return null;
  }

  private String getStatsTag(String statsField) {
    StringBuilder sb = new StringBuilder();
    sb.append("{!key=");
    sb.append(statsField + "_val");
    sb.append(" ");
    sb.append("tag=" + statsField + "_tag");
    sb.append("}" + statsField);

    return sb.toString();
  }

  public void setSolrClient(SolrClient solrClient) {
    this.solrClient = solrClient;
  }
}
