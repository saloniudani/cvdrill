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

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.store.solr.schema.SolrSchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrAggrQueryBuilder extends
AbstractExprVisitor<SolrAggrParam, Void, RuntimeException> {

  static final Logger logger = LoggerFactory.getLogger(SolrAggrQueryBuilder.class);

  final SolrGroupScan groupScan;
  final LogicalExpression le;
  private boolean allExpressionsConverted = true;

  public SolrAggrQueryBuilder(SolrGroupScan solrGroupScan, LogicalExpression conditionExp) {
    this.groupScan = solrGroupScan;
    this.le = conditionExp;
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  public SolrAggrParam parseTree() {
    SolrAggrParam parsedSpec = le.accept(this, null);
    return parsedSpec;
  }

  @Override
  public SolrAggrParam visitBooleanOperator(BooleanOperator op, Void valueArg) {
    return null;
  }

  @Override
  public SolrAggrParam visitFunctionCall(FunctionCall call, Void valueArg) throws RuntimeException {

    SolrAggrFunctionProcessor evaluator = SolrAggrFunctionProcessor.process(call);
    if (evaluator != null) {
      SolrAggrQueryBuilder.logger.debug("SolrAggrQueryBuilder :: visitFunctionCall");
      SolrAggrQueryBuilder.logger.debug("Function: " + evaluator.getFunctionName() + " Path: "
          + evaluator.getPath());
      SolrAggrQueryBuilder.logger.debug("Evaluator isSuccess : " + evaluator.isSuccess());

      SolrAggrParam solrAggrParam = new SolrAggrParam();
      solrAggrParam.setFunctionName(evaluator.getFunctionName());
      if (evaluator.isSuccess() && evaluator.getPath() != null) {
        String pathStr = evaluator.getPath().getAsUnescapedPath();
        List<SolrSchemaField> oSchemaFields = groupScan.getSolrScanSpec().getCvSchema()
            .getSchemaFields();
        boolean isValidField = false;
        for (SolrSchemaField solrSchemaField : oSchemaFields) {
          if (solrSchemaField.getFieldName().equalsIgnoreCase(pathStr)) {
            isValidField = true;
            break;
          }
        }
        if (!isValidField) {
          pathStr = SolrPluginConstants.DRILL_AGGREGATE_EXPR0;
          if (evaluator.getFunctionName().equalsIgnoreCase("sum")) {
            solrAggrParam.setFunctionName("count");
          }
        }
        solrAggrParam.setFieldName(pathStr);

      } else {
        solrAggrParam.setFieldName(SolrPluginConstants.DRILL_AGGREGATE_EXPR0);
      }
      solrAggrParam.setDataType(TypeProtos.MinorType.BIGINT);

      return solrAggrParam;
    } else {
      allExpressionsConverted = false;
    }
    return null;
  }

  @Override
  public SolrAggrParam visitFunctionHolderExpression(FunctionHolderExpression fhe, Void valueArg) {
    SolrAggrQueryBuilder.logger.info("SolrAggrQueryBuilder :: visitFunctionHolderExpression");
    allExpressionsConverted = false;
    return null;

  }

  @Override
  public SolrAggrParam visitUnknown(LogicalExpression e, Void valueArg) throws RuntimeException {
    SolrAggrQueryBuilder.logger.info("SolrAggrQueryBuilder :: visitUnknown");
    allExpressionsConverted = false;
    return null;
  }
}
