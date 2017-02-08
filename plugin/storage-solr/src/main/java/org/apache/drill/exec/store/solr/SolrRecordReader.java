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
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.Charsets;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.solr.schema.SolrSchemaField;
import org.apache.drill.exec.store.solr.schema.SolrSchemaResp;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.FieldStatsInfo;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SolrRecordReader extends AbstractRecordReader {
  static final Logger logger = LoggerFactory.getLogger(SolrRecordReader.class);
  private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_DATE_TIME;
  private static final String defaultDateFormat = "EEE MMM dd kk:mm:ss z yyyy";
  private FragmentContext fc;
  protected Map<String, ValueVector> vectors = null;
  protected String solrServerUrl;
  protected SolrClient solrClient;
  protected SolrSubScan solrSubScan;
  protected List<SolrScanSpec> scanList;
  protected SolrClientAPIExec solrClientApiExec;
  protected OutputMutator outputMutator;
  protected Iterator<SolrDocument> resultDocsIter;
  protected List<String> fields;
  private MajorType.Builder t;
  private Map<String, SolrSchemaField> schemaFieldMap;
  private List<SolrAggrParam> solrAggrParams = null;
  private boolean solrStreamReadFinished = false;
  private boolean useSolrStream = false;
  private SolrScanSpec solrScanSpec = null;
  int pivotResultCounter = 0;
  int resultCounter = 0;
  private String cursorMark = SolrPluginConstants.SOLR_DEFAULT_CURSOR_MARK;

  public SolrRecordReader(FragmentContext context, SolrSubScan config) {
    fc = context;
    solrSubScan = config;

    solrServerUrl = solrSubScan.getSolrPlugin().getSolrStorageConfig().getSolrServer();
    scanList = solrSubScan.getScanList();
    solrClient = solrSubScan.getSolrPlugin().getSolrClient();
    solrClientApiExec = new SolrClientAPIExec(solrClient);
    solrScanSpec = config.getSolrScanSpec();
    useSolrStream = solrSubScan.getSolrPlugin().getSolrStorageConfig().getSolrStorageProperties()
        .isUseSolrStream();
    solrAggrParams = config.getSolrScanSpec().getAggrParams();

    if (useSolrStream) {
      useSolrStream = !config.getSolrScanSpec().isAggregateQuery();
    }

    // solr core schema
    SolrSchemaResp oCVSchema = config.getSolrScanSpec().getCvSchema();
    List<SchemaPath> colums = config.getColumns();

    if (oCVSchema.getSchemaFields() != null) {
      schemaFieldMap = new HashMap<String, SolrSchemaField>(oCVSchema.getSchemaFields().size());

      for (SolrSchemaField cvSchemaField : oCVSchema.getSchemaFields()) {
        schemaFieldMap.put(cvSchemaField.getFieldName(), cvSchemaField);
      }
    }

    setColumns(colums);
  }

  @Override
  public void close() {
  }

  private void createVectorField(String fieldName, OutputMutator output, int idx)
      throws SchemaChangeException {
    MaterializedField m_field = null;
    SolrSchemaField cvSchemaField = schemaFieldMap.get(fieldName);
    Preconditions.checkNotNull(cvSchemaField);

    String key = fieldName;
    switch (cvSchemaField.getType()) {
    case "string":
      t = MajorType.newBuilder().setMinorType(MinorType.VARCHAR);
      m_field = MaterializedField.create(fieldName, t.build());
      vectors.put(key, output.addField(m_field, NullableVarCharVector.class));

      break;

    case "long":
    case "tlong":
    case "rounded1024":
    case "double":
    case "tdouble":
      t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.BIGINT);
      m_field = MaterializedField.create(fieldName, t.build());
      vectors.put(key, output.addField(m_field, NullableBigIntVector.class));

      break;

    case "int":
      t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.INT);
      m_field = MaterializedField.create(fieldName, t.build());
      vectors.put(key, output.addField(m_field, NullableIntVector.class));

      break;
    case "tfloat":
    case "float":
      t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.FLOAT8);
      m_field = MaterializedField.create(fieldName, t.build());
      vectors.put(key, output.addField(m_field, NullableFloat8Vector.class));

      break;

    case "date":
    case "tdate":
    case "epoch":
    case "timestamp":
      t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.TIMESTAMP);
      m_field = MaterializedField.create(fieldName, t.build());
      vectors.put(key, output.addField(m_field, NullableTimeStampVector.class));

      break;

    default:
      t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.VARCHAR);
      m_field = MaterializedField.create(fieldName, t.build());
      vectors.put(key, output.addField(m_field, NullableVarCharVector.class));

      break;
    }
  }

  private String getSolrField(String statsKey, String uniqueKey) {
    if ((statsKey != null) && !schemaFieldMap.containsKey(statsKey)) {
      return uniqueKey;
    }

    return statsKey;
  }

  private boolean isCountOnlyQuery() {
    int counterForCountQuery = 0;
    boolean isCountOnlyQuery = false;
    for (SolrAggrParam solrAggrParam : solrAggrParams) {
      String functionName = solrAggrParam.getFunctionName();
      if (functionName.equalsIgnoreCase("count")) {
        counterForCountQuery++;
      }
    }
    if (counterForCountQuery != 0 && counterForCountQuery == solrAggrParams.size()) {
      isCountOnlyQuery = true;
    }
    return isCountOnlyQuery;
  }

  @Override
  public int next() {
    int counter = 0;
    int statsCounter = 0;
    SolrRecordReader.logger.info("Use SOLR Stream : " + useSolrStream);

    QueryResponse rsp = null;
    String solrUrl = solrSubScan.getSolrScanSpec().getSolrUrl();
    String uniqueKey = solrSubScan.getSolrScanSpec().getCvSchema().getUniqueKey();
    String solrCoreName = solrSubScan.getSolrScanSpec().getSolrCoreName();
    SolrFilterParam filters = solrSubScan.getSolrScanSpec().getFilter();
    List<SolrSortParam> solrSortParams = solrSubScan.getSolrScanSpec().getSortParams();
    ArrayList<String> fieldListCopy = new ArrayList(this.fields);
    long solrDocFetchCount = solrSubScan.getSolrScanSpec().getSolrDocFetchCount(solrCoreName,
        fieldListCopy);
    boolean isDistinct = solrSubScan.getSolrScanSpec().isDistinct();
    boolean isGroup = solrSubScan.getSolrScanSpec().isGroup();
    boolean useFacetPivotFromGroupCount = isCountOnlyQuery() && isGroup;
    boolean isDataQuery = solrAggrParams.isEmpty() && !isGroup && !useFacetPivotFromGroupCount;


    if (!solrStreamReadFinished) {
      StringBuilder filterBuilder = new StringBuilder();
      if (filters != null) {
        for (String filter : filters) {
          filterBuilder.append(filter);
        }
      }

      if ((solrUrl != null) && solrUrl.contains(solrCoreName)) {
        solrServerUrl = solrUrl;
        solrCoreName = null; // setting it null since solrUrl already contains the coreName;
      }

      if (useSolrStream) {
        SolrStream solrStream = solrClientApiExec.getSolrStreamResponse(solrServerUrl,
            solrCoreName, this.fields, filterBuilder, uniqueKey, solrDocFetchCount);

        counter = processSolrStream(solrStream);
      } else {

        if (!solrAggrParams.isEmpty() && !isGroup && isCountOnlyQuery()) {
          SolrRecordReader.logger.info("Processing COUNT only query...");
          ValueVector vv1 = vectors.get(String.valueOf(statsCounter));
          processCountQuery(vv1, solrDocFetchCount, statsCounter, counter);
          statsCounter++;
          counter++; // actual record counter
        } else {
          if (solrDocFetchCount != 0) {
            if (isDistinct) {
              // make facet query.
            }

            if (isDataQuery && !solrSubScan.getSolrScanSpec().isLimitApplied()) {
              solrDocFetchCount = SolrPluginConstants.SOLR_DEFAULT_FETCH_COUNT;
            }
            rsp = solrClientApiExec.getSolrDocs(solrServerUrl, solrCoreName, uniqueKey,
                this.fields, solrDocFetchCount, cursorMark, filterBuilder, solrAggrParams,
                solrSortParams, solrScanSpec.getProjectFieldNames(), isGroup, isCountOnlyQuery());

            SolrDocumentList solrDocList = rsp.getResults();
            Map<String, FieldStatsInfo> fieldStatsInfoMap = rsp.getFieldStatsInfo();
            NamedList<List<PivotField>> facetPivots = rsp.getFacetPivot();

            // solr doc iteration
            if (solrDocList != null) {
              counter = processSolrDocs(solrDocList);
            }
            // facet iteration (for single group by)
            if (rsp.getFacetFields() != null) {
              counter = processFacetResponse(rsp, isCountOnlyQuery());
            }
            // stats/stats.facet iteration (for stats with single group by should be replace by
            // facet pivot)
            if (fieldStatsInfoMap != null) {
              counter = processStatsFieldResponse(fieldStatsInfoMap, isGroup, uniqueKey);
            }
            // facet pivot response
            if (facetPivots != null) {
              List<PivotField> pivotList = facetPivots.get(Joiner.on(",").join(fields));
              counter = processFacetPivotResponse(pivotList);
            }
          }
        }
      }

    }
    for (String key : vectors.keySet()) {
      ValueVector vv = vectors.get(key);
      vv.getMutator().setValueCount(counter);
    }
    if (isDataQuery && rsp != null && cursorMark != rsp.getNextCursorMark()) {
      solrStreamReadFinished = false;
      cursorMark = rsp.getNextCursorMark();
      SolrRecordReader.logger
      .info("Retrieving resultset from SOLR is not yet finished. Retriving with next cursor [ "
          + cursorMark + " ] with rows:" + solrDocFetchCount);
    } else {
      solrStreamReadFinished = true;
    }
    return counter;
  }

  private void processCountQuery(ValueVector vv1, Long solrDocFetchCount, int statsRecordCounter,
      int recordCounter) {
    processRecord(vv1, solrDocFetchCount, recordCounter);
  }

  private int processFacetPivotResponse(List<PivotField> facetPivots) {
    SolrFacetPivot2ResultSet oSolrFacetPivot2ResultSet = new SolrFacetPivot2ResultSet();
    oSolrFacetPivot2ResultSet.build(facetPivots);
    int resultSeek = 0;
    int leafNodesSeek = 0;
    while (!oSolrFacetPivot2ResultSet.isEmpty()) {
      SolrFacetPivotRecord oSolrFacetPivotRecord = (SolrFacetPivotRecord) oSolrFacetPivot2ResultSet
          .pop();
      PivotField pivotField = oSolrFacetPivotRecord.getPivotField();
      String fieldName = pivotField.getField();
      Object fieldValue = pivotField.getValue();
      ValueVector vv = vectors.get(fieldName);
      if (oSolrFacetPivotRecord.getPivotSize() == 0) {
        Map<String, FieldStatsInfo> fieldStatsInfo = pivotField.getFieldStatsInfo();
        processRecord(vv, fieldValue, leafNodesSeek);
        int statsCounter = 0;

        if (isCountOnlyQuery()) {
          processCountQuery(vectors.get(String.valueOf(statsCounter)),
              Long.valueOf(pivotField.getCount()), statsCounter, leafNodesSeek);
          statsCounter++;
        } else {
          for (SolrAggrParam solrAggrParam : solrAggrParams) {
            FieldStatsInfo fieldStats = fieldStatsInfo.get(getSolrField(
                solrAggrParam.getFieldName(), solrScanSpec.getCvSchema().getUniqueKey()));
            String functionName = solrAggrParam.getFunctionName();

            if (fieldStats != null) {
              processStatsRecord(fieldStats, functionName, statsCounter, leafNodesSeek,
                  solrScanSpec.isGroup());
              statsCounter++;

              if (statsCounter == solrScanSpec.getProjectFieldNames().size()) {
                break;
              }
            }
          }
        }
        leafNodesSeek++;
      } else {

        int start = (leafNodesSeek == oSolrFacetPivotRecord.getPivotSize()) ? 0 : Math
            .abs((leafNodesSeek - oSolrFacetPivotRecord.getPivotSize()));

        int end = (start == 0) ? leafNodesSeek : (start + oSolrFacetPivotRecord.getPivotSize());

        SolrRecordReader.logger.info("Start is : " + start + " End is : " + end);
        for (int i = start; i < end; i++) {
          processRecord(vv, fieldValue, i);
        }
        resultSeek += oSolrFacetPivotRecord.getPivotSize();
      }
    }
    return (resultSeek == 0) ? leafNodesSeek : resultSeek;
  }

  private int processFacetResponse(QueryResponse rsp, boolean isFacetCoutRequired) {
    int counter = 0;
    Object fieldValue = null;
    int statsCounter = 0;
    int idx = 0;
    for (String fieldName : fields) {
      FacetField facetField = rsp.getFacetField(fieldName);
      if (facetField != null) {
        boolean isSolrSchemaField = solrScanSpec.getProjectFieldNames().contains(fieldName);
        ValueVector vv = isSolrSchemaField ? vectors.get(fieldName) : vectors.get(String
            .valueOf(idx));

        fieldValue = facetField.getName();
        List<Count> facetValues = facetField.getValues();
        for (Count facetValue : facetValues) {
          fieldValue = facetValue.getName();
          processRecord(vv, fieldValue, counter);
          if (isFacetCoutRequired) {
            ValueVector vv1 = vectors.get(String.valueOf(statsCounter));
            long facetCount = facetValue.getCount();
            processCountQuery(vv1, facetCount, statsCounter, counter);
          }
          counter++;
        }
        statsCounter++;
        idx++;
      }
    }

    return counter;
  }

  private void processRecord(ValueVector vv, Object fieldValue, int recordCounter) {
    String fieldValueStr = null;
    byte[] record = null;
    try {
      fieldValueStr = fieldValue.toString();
      record = fieldValueStr.getBytes(Charsets.UTF_8);

      if (vv.getClass().equals(NullableVarCharVector.class)) {
        NullableVarCharVector v = (NullableVarCharVector) vv;
        v.getMutator().setSafe(recordCounter, record, 0, record.length);
        v.getMutator().setValueLengthSafe(recordCounter, record.length);
      } else if (vv.getClass().equals(NullableBigIntVector.class)) {
        NullableBigIntVector v = (NullableBigIntVector) vv;
        BigDecimal bd = new BigDecimal(fieldValueStr);
        v.getMutator().setSafe(recordCounter, bd.longValue());
      } else if (vv.getClass().equals(NullableIntVector.class)) {
        NullableIntVector v = (NullableIntVector) vv;
        v.getMutator().setSafe(recordCounter, Integer.parseInt(fieldValueStr));
      } else if (vv.getClass().equals(NullableFloat8Vector.class)) {
        NullableFloat8Vector v = (NullableFloat8Vector) vv;
        Double d = Double.parseDouble(fieldValueStr);
        v.getMutator().setSafe(recordCounter, d);
      } else if (vv.getClass().equals(DateVector.class)) {
        DateVector v = (DateVector) vv;
        long dtime = 0L;
        try {
          TemporalAccessor accessor = SolrRecordReader.timeFormatter.parse(fieldValueStr);
          Date date = Date.from(Instant.from(accessor));
          dtime = date.getTime();
        } catch (Exception e) {
          SimpleDateFormat dateParser = new SimpleDateFormat(SolrRecordReader.defaultDateFormat);
          dtime = dateParser.parse(fieldValueStr).getTime();
        }

        v.getMutator().setSafe(recordCounter, dtime);
      } else if (vv.getClass().equals(NullableTimeStampVector.class)) {
        NullableTimeStampVector v = (NullableTimeStampVector) vv;
        DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_DATE_TIME;
        long dtime = 0L;

        try {
          TemporalAccessor accessor = timeFormatter.parse(fieldValueStr);
          Date date = Date.from(Instant.from(accessor));
          dtime = date.getTime();
        } catch (Exception e) {
          SimpleDateFormat dateParser = new SimpleDateFormat(SolrRecordReader.defaultDateFormat);
          dtime = dateParser.parse(fieldValueStr).getTime();
        }
        v.getMutator().setSafe(recordCounter, dtime);
      }
    } catch (Exception e) {
      SolrRecordReader.logger.error("Error processing record: " + e.getMessage()
      + vv.getField().getPath() + " Field type " + vv.getField().getType() + " "
      + vv.getClass());
    }
  }

  private int processSolrDocs(SolrDocumentList solrDocList) {
    int counter = 0;
    Object fieldValue = null;
    for (SolrDocument solrDocument : solrDocList) {
      for (String columns : vectors.keySet()) {
        ValueVector vv = vectors.get(columns);
        fieldValue = solrDocument.get(columns);
        processRecord(vv, fieldValue, counter);
      }

      counter++;
    }
    return counter;
  }

  private int processSolrStream(SolrStream solrStream) {
    int recordCounter = 0;
    try {
      solrStream.open();
      Tuple solrDocument = null;

      while (true) {
        solrDocument = solrStream.read();
        if (solrDocument.EOF) {
          break;
        }
        for (String columns : vectors.keySet()) {
          ValueVector vv = vectors.get(columns);
          Object fieldValue = solrDocument.get(columns);
          processRecord(vv, fieldValue, recordCounter);
        }

        recordCounter++;
      }
    } catch (Exception e) {
      SolrRecordReader.logger.info("Error occured while fetching results from solr server "
          + e.getMessage());
      solrStreamReadFinished = true;

      return 0;
    } finally {
      try {
        solrStream.close();
        solrStream = null;
      } catch (IOException e) {
        solrStreamReadFinished = true;
        SolrRecordReader.logger.debug("Error occured while fetching results from solr server "
            + e.getMessage());
      }
    }

    return recordCounter;
  }

  private int processStatsFieldResponse(Map<String, FieldStatsInfo> fieldStatsInfoMap,
      boolean isGroup, String uniqueKey) {
    int counter = 0;
    int statsCounter = 0;
    Object fieldValue = null;
    if (isGroup) {
      for (SolrAggrParam solrAggrParam : solrAggrParams) {
        String statsKey = solrAggrParam.getFieldName();
        String functionName = solrAggrParam.getFunctionName();
        FieldStatsInfo fieldStats = fieldStatsInfoMap.get(getSolrField(statsKey, uniqueKey));
        Map<String, List<FieldStatsInfo>> statsFacet = fieldStats.getFacets();

        for (String groupKey : statsFacet.keySet()) {
          counter = 0;
          List<FieldStatsInfo> facetStats = statsFacet.get(groupKey);
          ValueVector vv = vectors.get(groupKey);
          if (vv != null) {
            for (FieldStatsInfo fieldStatsInfo : facetStats) {
              fieldValue = fieldStatsInfo.getName();
              processRecord(vv, fieldValue, counter);
              processStatsRecord(fieldStatsInfo, functionName, statsCounter, counter, isGroup);
              counter++; // actual record counter
            }
          }
        }

        statsCounter++;
      }
    } else {
      for (SolrAggrParam solrAggrParam : solrAggrParams) {
        FieldStatsInfo fieldStats = fieldStatsInfoMap.get(getSolrField(
            solrAggrParam.getFieldName(), uniqueKey));
        String functionName = solrAggrParam.getFunctionName();

        if (fieldStats != null) {
          processStatsRecord(fieldStats, functionName, statsCounter, counter, isGroup);
          statsCounter++;

          if (statsCounter == solrScanSpec.getProjectFieldNames().size()) {
            break;
          }
        }
      }

      counter++; // actual record counter
    }
    return counter;
  }

  private void processStatsRecord(FieldStatsInfo fieldStatsInfo, String functionName,
      int statsRecordCounter, int recordCounter, boolean isGroup) {
    Object fieldValue = null;
    ValueVector vv1 = vectors.get(String.valueOf(statsRecordCounter));

    if (vv1 != null) {
      if (functionName.equalsIgnoreCase("sum")) {
        fieldValue = fieldStatsInfo.getSum();
      } else if (functionName.equalsIgnoreCase("count")) {
        fieldValue = fieldStatsInfo.getCount();
      } else if (functionName.equalsIgnoreCase("min")) {
        fieldValue = fieldStatsInfo.getMin();
      } else if (functionName.equalsIgnoreCase("max")) {
        fieldValue = fieldStatsInfo.getMax();
      } else if (functionName.equalsIgnoreCase("avg")) {
        fieldValue = fieldStatsInfo.getMean();
      } else {
        SolrRecordReader.logger.debug("Yet to implement function type [ " + functionName + " ]");
      }
    }
    processRecord(vv1, fieldValue, recordCounter);
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    try {
      vectors = new HashMap<String, ValueVector>(fields.size());

      boolean isGroup = solrScanSpec.isGroup(); // if true build the column set
      boolean isAggregate = solrScanSpec.isAggregateQuery(); // if true but group is false don't
      // create column set
      int idx = 0;
      if (!isAggregate && !isGroup) {
        for (String field : fields) {
          createVectorField(field, output, idx);
        }
      }

      if (solrScanSpec.getProjectFieldNames().size() > 0) {
        for (String fieldName : solrScanSpec.getProjectFieldNames()) {
          if (!fields.contains(fieldName)) {
            if (!solrScanSpec.getAggrParams().isEmpty()) {
              for (SolrAggrParam aggrParam : solrScanSpec.getAggrParams()) {
                ValueVector vv = vectors.get(String.valueOf(idx));
                boolean isExists = (vv != null
                    && vv.getField().getPath().equalsIgnoreCase(fieldName));
                if (aggrParam.getDataType() != null && !isExists) {
                  t = MajorType.newBuilder().setMinorType(aggrParam.getDataType());
                  MaterializedField m_field = MaterializedField.create(fieldName, t.build());
                  vectors.put(String.valueOf(idx),
                      output.addField(m_field, NullableBigIntVector.class));
                  break;
                }
              }
            } else {
              t = MajorType.newBuilder().setMinorType(TypeProtos.MinorType.VARCHAR);
              MaterializedField m_field = MaterializedField.create(fieldName, t.build());
              vectors.put(String.valueOf(idx),
                  output.addField(m_field, NullableVarCharVector.class));
            }
            idx++;
          } else {
            createVectorField(fieldName, output, idx);
          }
        }
      }

      this.outputMutator = output;
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    boolean isGroup = solrSubScan.getSolrScanSpec().isGroup();

    if (!isStarQuery()) {
      SolrRecordReader.logger
      .debug("This is not a star query, restricting response to projected columns only "
          + projectedColumns);
      fields = Lists.newArrayListWithExpectedSize(projectedColumns.size());

      for (SchemaPath column : projectedColumns) {
        String fieldName = column.getRootSegment().getPath();

        if (schemaFieldMap.containsKey(fieldName)) {
          transformed.add(SchemaPath.getSimplePath(fieldName));
          this.fields.add(fieldName);
        }
      }
    } else {
      fields = Lists.newArrayListWithExpectedSize(schemaFieldMap.size());

      for (String fieldName : schemaFieldMap.keySet()) {
        this.fields.add(fieldName);
      }

      transformed.add(AbstractRecordReader.STAR_COLUMN);
    }

    return transformed;
  }
}
