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

import java.text.MessageFormat;
import java.util.List;

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions.QuotedString;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.store.solr.schema.SolrSchemaField;
import org.apache.drill.exec.store.solr.schema.SolrSchemaResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;


public class SolrQueryBuilder extends AbstractExprVisitor<SolrScanSpec, Void, RuntimeException> {
  static final Logger logger = LoggerFactory.getLogger(SolrQueryBuilder.class);
  private static final String SINGLE_QUOTE = "\'";
  private static final String DOUBLE_QUOTE = "\"";

  /**
   * Put quotes around the given String if necessary.
   *
   * If the argument doesn't include spaces or quotes, return it as is. If it contains double
   * quotes, use single quotes - else surround the argument by double quotes.
   *
   *
   * @param argument
   *          the argument to be quoted
   * @return the quoted argument
   * @throws IllegalArgumentException
   *           If argument contains both types of quotes
   */
  public static String quoteArgument(final String argument) {
    String cleanedArgument = argument.trim();

    while (cleanedArgument.startsWith(SolrQueryBuilder.SINGLE_QUOTE)
        || cleanedArgument.startsWith(SolrQueryBuilder.DOUBLE_QUOTE)) {
      cleanedArgument = cleanedArgument.substring(1);
    }

    while (cleanedArgument.endsWith(SolrQueryBuilder.SINGLE_QUOTE)
        || cleanedArgument.endsWith(SolrQueryBuilder.DOUBLE_QUOTE)) {
      cleanedArgument = cleanedArgument.substring(0, cleanedArgument.length() - 1);
    }

    final StringBuffer buf = new StringBuffer();

    if (cleanedArgument.indexOf(SolrQueryBuilder.DOUBLE_QUOTE) > -1) {
      if (cleanedArgument.indexOf(SolrQueryBuilder.SINGLE_QUOTE) > -1) {
        throw new IllegalArgumentException("Can't handle single and double quotes in same argument");
      } else {
        return buf.append(SolrQueryBuilder.SINGLE_QUOTE).append(cleanedArgument)
            .append(SolrQueryBuilder.SINGLE_QUOTE).toString();
      }
    } else if ((cleanedArgument.indexOf(SolrQueryBuilder.SINGLE_QUOTE) > -1)
        || (cleanedArgument.indexOf(" ") > -1)) {
      return buf.append(SolrQueryBuilder.DOUBLE_QUOTE).append(cleanedArgument)
          .append(SolrQueryBuilder.DOUBLE_QUOTE).toString();
    } else {
      return cleanedArgument;
    }
  }

  final SolrGroupScan groupScan;
  final LogicalExpression le;

  private boolean allExpressionsConverted = true;

  public SolrQueryBuilder(SolrGroupScan solrGroupScan,
      LogicalExpression conditionExp) {
    this.groupScan = solrGroupScan;
    this.le = conditionExp;
  }

  public SolrScanSpec createSolrScanSpec(String functionName, SchemaPath field, Object fieldValue) {
    // extract the field name
    String fieldName = field.getAsUnescapedPath();
    SolrFilterParam filterParam = null;
    String operator = null;

    switch (functionName) {
    case "equal":
      operator = ":{0}";

      break;

    case "not_equal":
      operator = "-{0}";

      break;

    case "greater_than_or_equal_to":
      operator = ":[{0} TO *]";

      break;

    case "greater_than":
      operator = ":'{'{0} TO *'}'";

      break;

    case "less_than_or_equal_to":
      operator = ":[* TO {0}]";

      break;

    case "less_than":
      operator = ":'{'* TO {0}'}'";

      break;

    case "isnull":
    case "isNull":
    case "is null":

    case "isnotnull":
    case "isNotNull":
    case "is not null":
    default:
      allExpressionsConverted = false;

      break;
    }

    if (operator != null) {
      SolrQueryBuilder.logger.info(" fieldValue " + fieldValue + " operator " + operator);

      String fieldValueStr = SolrQueryBuilder.quoteArgument(fieldValue.toString());
      fieldValue = MessageFormat.format(operator, fieldValueStr);
      filterParam = new SolrFilterParam(fieldName, fieldValue.toString());
    }

    SolrQueryBuilder.logger.debug("FieldName " + fieldName + " :: FunctionName " + functionName);

    SolrScanSpec solrScanSpec = null;

    if (filterParam != null) {
      solrScanSpec = new SolrScanSpec(this.groupScan.getSolrScanSpec().getSolrCoreName(),
          filterParam);
    } else {
      solrScanSpec = new SolrScanSpec(this.groupScan.getSolrScanSpec().getSolrCoreName());
    }

    solrScanSpec.setCvSchema(this.groupScan.getSolrScanSpec().getCvSchema());
    solrScanSpec.setSolrUrl(this.groupScan.getSolrScanSpec().getSolrUrl());

    return null;
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  private boolean isComparisonAllowed(String fieldName) {
    SolrSchemaResp solrSchema = this.groupScan.getSolrScanSpec().getCvSchema();
    List<SolrSchemaField> schemaFields = solrSchema.getSchemaFields();
    boolean isComparisonAllowed = true;

    for (SolrSchemaField solrSchemaField : schemaFields) {
      if (solrSchemaField.getFieldName().equalsIgnoreCase(fieldName)) {
        String solrFieldType = solrSchemaField.getType();

        if (solrFieldType.equals("string") || solrFieldType.equals("commaDelimited")
            || solrFieldType.equals("text_general") || solrFieldType.equals("currency")
            || solrFieldType.equals("uuid")) {
          isComparisonAllowed = false;

          break;
        }
      }
    }

    return isComparisonAllowed;
  }

  public SolrScanSpec mergeScanSpecs(String functionName,
      SolrScanSpec leftScanSpec,
      SolrScanSpec rightScanSpec) {
    SolrFilterParam solrFilter = new SolrFilterParam();
    SolrQueryBuilder.logger.info("MergeScanSpecs ::" + leftScanSpec.getFilter() + " "
        + rightScanSpec.getFilter());

    switch (functionName) {
    case "booleanAnd":

      if ((leftScanSpec.getFilter().size() > 0) && (rightScanSpec.getFilter().size() > 0)) {
        solrFilter.add(Joiner.on("").join(leftScanSpec.getFilter()));
        solrFilter.add(" AND ");
        solrFilter.add(Joiner.on("").join(rightScanSpec.getFilter()));
      } else if (leftScanSpec.getFilter().size() > 0) {
        solrFilter = leftScanSpec.getFilter();
      } else {
        solrFilter = rightScanSpec.getFilter();
      }

      break;

    case "booleanOr":
      solrFilter.add(Joiner.on("").join(leftScanSpec.getFilter()));
      solrFilter.add(" OR ");
      solrFilter.add(Joiner.on("").join(rightScanSpec.getFilter()));
    }

    SolrScanSpec solrScanSpec = new SolrScanSpec(groupScan.getSolrScanSpec()
        .getSolrCoreName(),
        solrFilter);
    solrScanSpec.setCvSchema(this.groupScan.getSolrScanSpec().getCvSchema());
    solrScanSpec.setSolrUrl(this.groupScan.getSolrScanSpec().getSolrUrl());

    return solrScanSpec;
  }

  public SolrScanSpec parseTree() {
    SolrScanSpec parsedSpec = le.accept(this, null);

    if (parsedSpec != null) {
      parsedSpec = mergeScanSpecs("booleanAnd",
          this.groupScan.getSolrScanSpec(), parsedSpec);
    }

    return parsedSpec;
  }

  @Override
  public SolrScanSpec visitBooleanOperator(BooleanOperator op, Void valueArg) {
    List<LogicalExpression> args = op.args;
    String functionName = op.getName();
    SolrScanSpec nodeScanSpec = null;
    SolrQueryBuilder.logger.debug("FunctionName :: " + functionName);

    for (int i = 0; i < args.size(); ++i) {
      SolrQueryBuilder.logger.info(" args " + args.get(i));

      switch (functionName) {
      case "booleanAnd":
      case "booleanOr":

        if (nodeScanSpec == null) {
          nodeScanSpec = args.get(i).accept(this, valueArg);
        } else {
          SolrScanSpec scanSpec = args.get(i).accept(this, valueArg);

          if (scanSpec != null) {
            nodeScanSpec = mergeScanSpecs(functionName, nodeScanSpec, scanSpec);
          } else {
            allExpressionsConverted = false;
          }
        }

        SolrQueryBuilder.logger.info("Expression Converted!");

        break;
      }
    }

    return nodeScanSpec;
  }

  @Override
  public SolrScanSpec visitFunctionCall(FunctionCall call, Void valueArg)
      throws RuntimeException {
    SolrScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    ImmutableList<LogicalExpression> args = call.args;
    LogicalExpression nameVal = call.args.get(0);
    LogicalExpression valueArg1 = (call.args.size() == 2) ? call.args.get(1) : null;

    String schemaFieldName = null;
    boolean isComparisonAllowed = true;

    if ((nameVal != null) && nameVal instanceof SchemaPath) {
      SchemaPath path = (SchemaPath) nameVal;
      schemaFieldName = path.getAsUnescapedPath();
    }

    if (schemaFieldName != null) {
      //isComparisonAllowed = isComparisonAllowed(schemaFieldName);
    }

    if (isComparisonAllowed) {
      if (SolrCompareFunctionProcessor.isCompareFunction(functionName)) {
        SolrQueryBuilder.logger.debug("visitFunctionCall :: FunctionName :: " + functionName);
        SolrQueryBuilder.logger.debug("visitFunctionCall :: valueArg1 :: " + valueArg1);

        SolrCompareFunctionProcessor evaluator = SolrCompareFunctionProcessor.process(call);

        if (evaluator.isSuccess()) {
          try {
            nodeScanSpec = createSolrScanSpec(evaluator.getFunctionName(), evaluator.getPath(),
                evaluator.getValue());
          } catch (Exception e) {
            SolrQueryBuilder.logger.debug("Failed to create filters ", e);
          }
        }
      } else {
        switch (functionName) {
        case "booleanAnd":
        case "booleanOr":

          SolrScanSpec leftScanSpec = args.get(0).accept(this, null);
          SolrScanSpec rightScanSpec = args.get(1).accept(this, null);

          if ((leftScanSpec != null) && (rightScanSpec != null)) {
            nodeScanSpec = mergeScanSpecs(functionName, leftScanSpec, rightScanSpec);
          } else {
            allExpressionsConverted = false;

            if ("booleanAnd".equals(functionName)) {
              nodeScanSpec = (leftScanSpec == null) ? rightScanSpec : leftScanSpec;
            }
          }

          break;
        }
      }
    } else {
      SolrQueryBuilder.logger.info("Skipping comparison on this field...");
    }

    if (nodeScanSpec == null) {
      allExpressionsConverted = false;
    }

    return nodeScanSpec;
  }

  @Override
  public SolrScanSpec visitFunctionHolderExpression(
      FunctionHolderExpression fhe, Void valueArg) {
    SolrQueryBuilder.logger.info("SolrQueryBuilder :: visitFunctionHolderExpression");

    return null;
  }

  @Override
  public SolrScanSpec visitQuotedStringConstant(QuotedString e, Void valueArg)
      throws RuntimeException {
    SolrQueryBuilder.logger.info("SolrQueryBuilder :: visitQuotedStringConstant");
    allExpressionsConverted = false;

    return null;
  }

  @Override
  public SolrScanSpec visitUnknown(LogicalExpression e, Void valueArg)
      throws RuntimeException {
    SolrQueryBuilder.logger.info("SolrQueryBuilder :: visitUnknown");
    allExpressionsConverted = false;

    return null;
  }
}
