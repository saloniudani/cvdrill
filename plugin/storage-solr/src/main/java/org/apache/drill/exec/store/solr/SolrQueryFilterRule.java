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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillAggregateRel;
import org.apache.drill.exec.planner.logical.DrillFilterRel;
import org.apache.drill.exec.planner.logical.DrillLimitRel;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillSortRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public abstract class SolrQueryFilterRule extends StoragePluginOptimizerRule {
  static final Logger logger = LoggerFactory.getLogger(SolrQueryFilterRule.class);
  static final ImmutableList<SqlTypeName> NoGroupingSupportOnDataType = ImmutableList.of(
      SqlTypeName.TIMESTAMP, SqlTypeName.INTEGER);

  public static final StoragePluginOptimizerRule FILTER_ON_SCAN = new SolrQueryFilterRule(
      RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)),
      "SolrQueryFilterRule:Filter_On_Scan") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(1);

      if (scan.getGroupScan() instanceof SolrGroupScan) {
        return super.matches(call);
      }

      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final DrillFilterRel filterRel = (DrillFilterRel) call.rel(0);
      final DrillScanRel scan = (DrillScanRel) call.rel(1);
      doOnMatch(call, filterRel, null, scan);
    }
  };

  /*
   * Move this class out
   */
  public static final StoragePluginOptimizerRule PROJECT_AGG_PUSH_DOWN = new SolrQueryFilterRule(
      RelOptHelper.some(DrillProjectRel.class,
          RelOptHelper.some(DrillAggregateRel.class, RelOptHelper.any(DrillScanRel.class))),
      "StoragePluginOptimizerRule:PROJECT_AGG_PUSH_DOWN") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(2);

      if (scan.getGroupScan() instanceof SolrGroupScan) {
        return super.matches(call);
      }

      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      SolrQueryFilterRule.logger.debug("SolrQueryFilterRule::PROJECT_AGG_PUSH_DOWN");

      final DrillProjectRel projectRel = (DrillProjectRel) call.rel(0);
      final DrillAggregateRel aggrRel = (DrillAggregateRel) call.rel(1);
      final DrillScanRel scanRel = (DrillScanRel) call.rel(2);
      doOnAggrMatch(call, projectRel, aggrRel, scanRel, null, null);
    }
  };

  public static final StoragePluginOptimizerRule AGG_PUSH_DOWN = new SolrQueryFilterRule(
      RelOptHelper.some(
          DrillProjectRel.class,
          RelOptHelper.some(DrillAggregateRel.class,
              RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class)))),
      "StoragePluginOptimizerRule:AGG_PUSH_DOWN") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(3);

      if (scan.getGroupScan() instanceof SolrGroupScan) {
        return super.matches(call);
      }

      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      SolrQueryFilterRule.logger.debug("SolrQueryFilterRule::AGG_PUSH_DOWN");

      final DrillProjectRel project = (DrillProjectRel) call.rel(0);
      final DrillAggregateRel aggregate = (DrillAggregateRel) call.rel(1);
      final DrillProjectRel project1 = (DrillProjectRel) call.rel(2);
      final DrillScanRel scan = (DrillScanRel) call.rel(3);
      doOnAggrMatch(call, project, aggregate, scan, null, null);
    }
  };

  public static final StoragePluginOptimizerRule AGG_PUSH_DOWN_LIMIT = new SolrQueryFilterRule(
      RelOptHelper.some(
          DrillProjectRel.class,
          RelOptHelper.some(DrillLimitRel.class,
              RelOptHelper.some(DrillAggregateRel.class, RelOptHelper.any(DrillScanRel.class)))),
      "StoragePluginOptimizerRule:AGG_PUSH_DOWN_LIMIT") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(3);

      if (scan.getGroupScan() instanceof SolrGroupScan) {
        return super.matches(call);
      }

      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      SolrQueryFilterRule.logger.debug("SolrQueryFilterRule::AGG_PUSH_DOWN_LIMIT");

      final DrillProjectRel projectRel = (DrillProjectRel) call.rel(0);
      final DrillLimitRel limiRel = (DrillLimitRel) call.rel(1);
      final DrillAggregateRel aggrRel = (DrillAggregateRel) call.rel(2);
      final DrillScanRel scanRel = (DrillScanRel) call.rel(3);
      doOnAggrMatch(call, projectRel, aggrRel, scanRel, limiRel, null);
    }
  };

  public static final StoragePluginOptimizerRule AGG_PUSH_DOWN_LIMIT2 = new SolrQueryFilterRule(
      RelOptHelper.some(
          DrillProjectRel.class,
          RelOptHelper.some(
              DrillLimitRel.class,
              RelOptHelper.some(DrillAggregateRel.class,
                  RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))))),
      "StoragePluginOptimizerRule:AGG_PUSH_DOWN_LIMIT2") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(4);

      if (scan.getGroupScan() instanceof SolrGroupScan) {
        return super.matches(call);
      }

      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      SolrQueryFilterRule.logger.debug("SolrQueryFilterRule::AGG_PUSH_DOWN_LIMIT2");

      final DrillProjectRel projectRel = (DrillProjectRel) call.rel(0);
      final DrillLimitRel limiRel = (DrillLimitRel) call.rel(1);
      final DrillAggregateRel aggrRel = (DrillAggregateRel) call.rel(2);
      final DrillProjectRel projectRel1 = (DrillProjectRel) call.rel(3);
      final DrillScanRel scanRel = (DrillScanRel) call.rel(4);
      doOnAggrMatch(call, projectRel, aggrRel, scanRel, limiRel, null);
    }
  };

  public static final StoragePluginOptimizerRule AGG_FILTER_PUSH_DOWN_LIMIT = new SolrQueryFilterRule(
      RelOptHelper
      .some(
          DrillProjectRel.class,
          RelOptHelper.some(
              DrillLimitRel.class,
              RelOptHelper.some(
                  DrillFilterRel.class,
                  RelOptHelper.some(
                      DrillAggregateRel.class,
                      RelOptHelper.some(DrillProjectRel.class,
                          RelOptHelper.any(DrillScanRel.class)))))),
      "StoragePluginOptimizerRule:AGG_FILTER_PUSH_DOWN_LIMIT") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(5);

      if (scan.getGroupScan() instanceof SolrGroupScan) {
        return super.matches(call);
      }

      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      SolrQueryFilterRule.logger.debug("SolrQueryFilterRule::AGG_FILTER_PUSH_DOWN_LIMIT");

      final DrillProjectRel project = (DrillProjectRel) call.rel(0);
      final DrillLimitRel limit = (DrillLimitRel) call.rel(1);
      final DrillFilterRel filter = (DrillFilterRel) call.rel(2);
      final DrillAggregateRel aggrRel = (DrillAggregateRel) call.rel(3);
      final DrillProjectRel project1 = (DrillProjectRel) call.rel(4);
      final DrillScanRel scan = (DrillScanRel) call.rel(5);

      doAggrFilterMatch(call, project, limit, filter, aggrRel, project1, scan);
    }
  };

  public static final StoragePluginOptimizerRule AGG_FILTER_PUSH_DOWN = new SolrQueryFilterRule(
      RelOptHelper.some(
          DrillProjectRel.class,
          RelOptHelper.some(
              DrillFilterRel.class,
              RelOptHelper.some(DrillAggregateRel.class,
                  RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))))),
      "StoragePluginOptimizerRule:AGG_FILTER_PUSH_DOWN") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(4);

      if (scan.getGroupScan() instanceof SolrGroupScan) {
        return super.matches(call);
      }

      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      SolrQueryFilterRule.logger.debug("SolrQueryFilterRule::AGG_FILTER_PUSH_DOWN");

      final DrillProjectRel project = (DrillProjectRel) call.rel(0);
      final DrillFilterRel filter = (DrillFilterRel) call.rel(1);
      final DrillAggregateRel aggrRel = (DrillAggregateRel) call.rel(2);
      final DrillProjectRel project1 = (DrillProjectRel) call.rel(3);
      final DrillScanRel scan = (DrillScanRel) call.rel(4);

      doAggrFilterMatch(call, project, null, filter, aggrRel, project1, scan);
    }
  };

  public static final StoragePluginOptimizerRule AGG_FILTER_PUSH_DOWN2 = new SolrQueryFilterRule(
      RelOptHelper.some(
          DrillProjectRel.class,
          RelOptHelper.some(
              DrillLimitRel.class,
              RelOptHelper.some(
                  DrillProjectRel.class,
                  RelOptHelper.some(
                      DrillFilterRel.class,
                      RelOptHelper.some(
                          DrillAggregateRel.class,
                          RelOptHelper.some(DrillProjectRel.class,
                              RelOptHelper.any(DrillScanRel.class))))))),
      "StoragePluginOptimizerRule:AGG_FILTER_PUSH_DOWN2") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(6);

      if (scan.getGroupScan() instanceof SolrGroupScan) {
        return super.matches(call);
      }

      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      SolrQueryFilterRule.logger.debug("SolrQueryFilterRule::AGG_FILTER_PUSH_DOWN2");

      final DrillProjectRel project = (DrillProjectRel) call.rel(0);
      final DrillLimitRel limit = (DrillLimitRel) call.rel(1);
      final DrillProjectRel project1 = (DrillProjectRel) call.rel(2);
      final DrillFilterRel filter = (DrillFilterRel) call.rel(3);
      final DrillAggregateRel aggrRel = (DrillAggregateRel) call.rel(4);
      final DrillProjectRel project2 = (DrillProjectRel) call.rel(5);
      final DrillScanRel scan = (DrillScanRel) call.rel(6);

      doAggrFilterMatch(call, project, limit, filter, aggrRel, project1, scan);
    }
  };
  public static final StoragePluginOptimizerRule AGG_FILTER_PUSH_DOWN3 = new SolrQueryFilterRule(
      RelOptHelper.some(
          DrillAggregateRel.class,
          RelOptHelper.some(DrillProjectRel.class,
              RelOptHelper.some(DrillFilterRel.class, RelOptHelper.any(DrillScanRel.class)))),
      "StoragePluginOptimizerRule:AGG_FILTER_PUSH_DOWN3") {

    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(3);

      if (scan.getGroupScan() instanceof SolrGroupScan) {
        return super.matches(call);
      }

      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      SolrQueryFilterRule.logger.debug("SolrQueryFilterRule::AGG_FILTER_PUSH_DOWN3");

      final DrillAggregateRel aggrRel = (DrillAggregateRel) call.rel(0);
      final DrillProjectRel project = (DrillProjectRel) call.rel(1);
      final DrillFilterRel filter = (DrillFilterRel) call.rel(2);
      final DrillScanRel scan = (DrillScanRel) call.rel(3);

      doAggrFilterMatch(call, project, null, filter, aggrRel, null, scan);
    }

  };

  public static final StoragePluginOptimizerRule AGG_PUSH_DOWN_WITH_SORT_LIMIT = new SolrQueryFilterRule(
      RelOptHelper.some(DrillProjectRel.class, RelOptHelper.some(
          DrillLimitRel.class,
          RelOptHelper.some(DrillSortRel.class,
              RelOptHelper.some(DrillAggregateRel.class, RelOptHelper.any(DrillScanRel.class))))),
      "StoragePluginOptimizerRule:AGG_PUSH_DOWN_WITH_SORT_LIMIT") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(4);

      if (scan.getGroupScan() instanceof SolrGroupScan) {
        return super.matches(call);
      }

      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      SolrQueryFilterRule.logger.debug("SolrQueryFilterRule::AGG_PUSH_DOWN_WITH_SORT_LIMIT");

      final DrillProjectRel project = (DrillProjectRel) call.rel(0);
      final DrillLimitRel limit = (DrillLimitRel) call.rel(1);
      final DrillSortRel sort = (DrillSortRel) call.rel(2);
      final DrillAggregateRel aggrRel = (DrillAggregateRel) call.rel(3);
      final DrillScanRel scan = (DrillScanRel) call.rel(4);

      doOnAggrMatch(call, project, aggrRel, scan, limit, sort);
    }
  };

  public static final StoragePluginOptimizerRule AGG_PUSH_DOWN_WITH_SORT = new SolrQueryFilterRule(
      RelOptHelper.some(
          DrillProjectRel.class,
          RelOptHelper.some(DrillSortRel.class,
              RelOptHelper.some(DrillAggregateRel.class, RelOptHelper.any(DrillScanRel.class)))),
      "StoragePluginOptimizerRule:AGG_PUSH_DOWN_WITH_SORT") {
    @Override
    public boolean matches(RelOptRuleCall call) {
      final DrillScanRel scan = (DrillScanRel) call.rel(3);

      if (scan.getGroupScan() instanceof SolrGroupScan) {
        return super.matches(call);
      }

      return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      SolrQueryFilterRule.logger.debug("SolrQueryFilterRule::AGG_PUSH_DOWN_WITH_SORT");

      final DrillProjectRel project = (DrillProjectRel) call.rel(0);
      final DrillSortRel sort = (DrillSortRel) call.rel(1);
      final DrillAggregateRel aggrRel = (DrillAggregateRel) call.rel(2);
      final DrillScanRel scan = (DrillScanRel) call.rel(3);

      doOnAggrMatch(call, project, aggrRel, scan, null, sort);
    }
  };

  public SolrQueryFilterRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  protected void doAggrFilterMatch(RelOptRuleCall call, DrillProjectRel project,
      DrillLimitRel limit, DrillFilterRel filter, DrillAggregateRel aggrRel,
      DrillProjectRel project1, DrillScanRel scan) {

    SolrGroupScan solrGroupScan = (SolrGroupScan) scan.getGroupScan();
    SolrScanSpec newScanSpec = solrGroupScan.getSolrScanSpec();
    List<SolrAggrParam> sAggrLst = Lists.newLinkedList();
    SolrQueryBuilder sQueryBuilder = null;
    boolean isAllFilterPushedDown = false;
    List<RelDataTypeField> relDataTypeField = scan.getRowType().getFieldList();

    boolean supportSolrAggregation = isSolrAggregationSupported(aggrRel, relDataTypeField);

    final int fetch = (limit != null) ? Math.max(0, RexLiteral.intValue(limit.getFetch())) : (-1);
    newScanSpec.setSolrDocFetchCount(fetch);
    if (fetch != -1) {
      newScanSpec.setLimitApplied(true);
    }

    DrillParseContext drillParseContext = new DrillParseContext(PrelUtil.getPlannerSettings(call
        .getPlanner()));
    if (aggrRel.getGroupCount() >= 1) {
      final RexNode condition = filter.getCondition();
      LogicalExpression conditionExp = DrillOptiq.toDrill(drillParseContext, scan, condition);

      sQueryBuilder = new SolrQueryBuilder(solrGroupScan, conditionExp);
      newScanSpec = sQueryBuilder.parseTree();
      isAllFilterPushedDown = sQueryBuilder.isAllExpressionsConverted();
    } else {
      SolrQueryFilterRule.logger
      .debug("Ignoring filter condition since query doesn't contain a group by");
    }

    if (supportSolrAggregation && newScanSpec != null) {

      boolean isAllAggregatePushedDown = pushAggregateToScan(solrGroupScan, aggrRel, sAggrLst);
      SolrQueryFilterRule.logger.info("All expressions converted : " + isAllAggregatePushedDown);
      setScanSpecForAggregation(drillParseContext, solrGroupScan, newScanSpec, sAggrLst, aggrRel,
          project, scan);
      SolrGroupScan newGroupScan = new SolrGroupScan(solrGroupScan.getUserName(),
          solrGroupScan.getSolrPlugin(), newScanSpec, solrGroupScan.getColumns());

      DrillScanRel newScanRel = new DrillScanRel(scan.getCluster(), scan.getTraitSet(),
          scan.getTable(), newGroupScan, project.getRowType(), scan.getColumns());

      DrillProjectRel newProjectRel = DrillProjectRel.create(project.getCluster(),
          project.getTraitSet(), newScanRel, project.getProjects(), project.getRowType());

      final RelNode childRel = (project == null) ? newScanRel : project.copy(project.getTraitSet(),
          ImmutableList.of((RelNode) newScanRel));

      if (isAllAggregatePushedDown || (sAggrLst.isEmpty() && newScanSpec.isGroup())) {
        SolrQueryFilterRule.logger.info("Transforming SQL query with optimizer rule.");
        call.transformTo(newProjectRel);
      }
    }
  }

  protected void doOnAggrMatch(RelOptRuleCall call, DrillProjectRel project,
      DrillAggregateRel aggregate, DrillScanRel scan, DrillLimitRel limit, DrillSortRel sort) {
    SolrGroupScan solrGroupScan = (SolrGroupScan) scan.getGroupScan();
    SolrScanSpec newScanSpec = solrGroupScan.getSolrScanSpec();
    List<SolrAggrParam> sAggrLst = Lists.newLinkedList();
    List<RelDataTypeField> relDataTypeField = scan.getRowType().getFieldList();

    boolean supportSolrAggregation = isSolrAggregationSupported(aggregate, relDataTypeField);
    List<SolrSortParam> solrSortParams = Lists.newArrayList();

    final int fetch = (limit != null) ? Math.max(0, RexLiteral.intValue(limit.getFetch())) : (-1);
    newScanSpec.setSolrDocFetchCount(fetch);
    if (fetch != -1) {
      newScanSpec.setLimitApplied(true);
    }
    DrillParseContext drillParseContext = new DrillParseContext(PrelUtil.getPlannerSettings(call
        .getPlanner()));
    if (sort != null) {
      final List<String> childFields = sort.getInput().getRowType().getFieldNames();

      for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {
        SqlMonotonicity monotonicity = fieldCollation.getDirection().monotonicity();
        String fieldName = childFields.get(fieldCollation.getFieldIndex());
        ORDER sortDir = getSolrSortDir(monotonicity);
        SolrSortParam solrSortParam = new SolrSortParam();
        solrSortParam.setSortFieldName(fieldName);
        solrSortParam.setSortDir(sortDir);
        solrSortParams.add(solrSortParam);
      }
    }

    newScanSpec.setSortParams(solrSortParams);

    if (supportSolrAggregation) {
      boolean isAllExpressionConverted = pushAggregateToScan(solrGroupScan, aggregate, sAggrLst);
      SolrQueryFilterRule.logger.info("All expressions converted : " + isAllExpressionConverted);
      setScanSpecForAggregation(drillParseContext, solrGroupScan, newScanSpec, sAggrLst, aggregate,
          project, scan);

      SolrGroupScan newGroupScan = new SolrGroupScan(solrGroupScan.getUserName(),
          solrGroupScan.getSolrPlugin(), newScanSpec, solrGroupScan.getColumns());

      DrillScanRel newScanRel = new DrillScanRel(scan.getCluster(), scan.getTraitSet(),
          scan.getTable(), newGroupScan, project.getRowType(), scan.getColumns());

      // Depending on whether is a project in the middle,
      // assign either scan or copy of project to childRel.
      final RelNode childRel = (project == null) ? newScanRel : project.copy(project.getTraitSet(),
          ImmutableList.of((RelNode) newScanRel));

      try {
        if (isAllExpressionConverted || (sAggrLst.isEmpty() && newScanSpec.isGroup())) {
          call.transformTo(childRel);
        }
      } catch (Exception e) {
        SolrQueryFilterRule.logger.error(e.getMessage());
      }
    }
  }

  protected void doOnMatch(RelOptRuleCall call, DrillFilterRel filterRel,
      DrillProjectRel projectRel, DrillScanRel scanRel) {
    DrillRel inputRel = (projectRel != null) ? projectRel : scanRel;
    SolrGroupScan solrGroupScan = (SolrGroupScan) scanRel.getGroupScan();
    final RexNode condition = filterRel.getCondition();

    LogicalExpression conditionExp = DrillOptiq.toDrill(
        new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scanRel, condition);

    SolrQueryBuilder sQueryBuilder = new SolrQueryBuilder(solrGroupScan, conditionExp);
    SolrScanSpec newScanSpec = sQueryBuilder.parseTree();

    if (newScanSpec == null) {
      return;
    }

    newScanSpec.setDataQuery(true);

    SolrGroupScan newGroupScan = new SolrGroupScan(solrGroupScan.getUserName(),
        solrGroupScan.getSolrPlugin(), newScanSpec, solrGroupScan.getColumns());

    DrillScanRel newScanRel = new DrillScanRel(scanRel.getCluster(), scanRel.getTraitSet().plus(
        DrillRel.DRILL_LOGICAL), scanRel.getTable(), newGroupScan, scanRel.getRowType(),
        scanRel.getColumns());

    if (projectRel != null) {
      DrillProjectRel newProjectRel = DrillProjectRel.create(projectRel.getCluster(),
          projectRel.getTraitSet(), newScanRel, projectRel.getProjects(), projectRel.getRowType());
      inputRel = newProjectRel;
    } else {
      inputRel = newScanRel;
    }

    try {
      if (sQueryBuilder.isAllExpressionsConverted()) {
        call.transformTo(inputRel);
      } else {
        call.transformTo(filterRel.copy(filterRel.getTraitSet(),
            ImmutableList.of((RelNode) inputRel)));
      }
    } catch (Exception e) {
      SolrQueryFilterRule.logger.error(e.getMessage());
    }
  }

  private ORDER getSolrSortDir(SqlMonotonicity sqlMonotonicity) {
    if (sqlMonotonicity.isDecreasing()) {
      return ORDER.desc;
    }

    return ORDER.asc;
  }

  private Boolean isGroupingAllowedOnField(DrillProjectRel project,
      List<SolrAggrParam> aggregateParams, List<RexNode> projects) {
    boolean isGroupingAllowedOnField = true;
    if (aggregateParams.isEmpty()) {
      for (RexNode rxn : projects) {
        if (SolrQueryFilterRule.NoGroupingSupportOnDataType
            .contains(rxn.getType().getSqlTypeName())) {
          isGroupingAllowedOnField = false;
          break;

        }
      }
    }
    SolrQueryFilterRule.logger.debug("isGroupingAllowedOnField: " + isGroupingAllowedOnField);
    return isGroupingAllowedOnField;
  }

  private boolean isSolrAggregationSupported(DrillAggregateRel aggregateRel,
      List<RelDataTypeField> relDataTypeFields) {
    List<AggregateCall> aggrCallList = (aggregateRel.getAggCallList() != null) ? aggregateRel
        .getAggCallList() : Lists.newArrayList();

        if (aggrCallList.isEmpty() && aggregateRel.getGroupCount() == 0) {
          SolrQueryFilterRule.logger.debug("Query contains no grouping...");

          return false;
        }

        if (!aggregateRel.getGroupType().equals(Group.SIMPLE)) {
          SolrQueryFilterRule.logger.debug("Not pushing group type of [ " + aggregateRel.getGroupType()
              + " ]");

          return false;
        }
        for (RelDataTypeField relDataTypeField : relDataTypeFields) {
          if (SolrQueryFilterRule.NoGroupingSupportOnDataType.contains(relDataTypeField.getType()
              .getSqlTypeName())) {
            return false;
          }
        }
        return true;
  }

  private boolean pushAggregateToScan(SolrGroupScan solrGroupScan, DrillAggregateRel aggregateRel,
      List<SolrAggrParam> sAggrLst) {
    List<String> aggrFields = aggregateRel.getInput().getRowType().getFieldNames();
    List<AggregateCall> aggrCallList = (aggregateRel.getAggCallList() != null) ? aggregateRel
        .getAggCallList() : Lists.newArrayList();
        boolean isAllExpressionConverted = false;

        for (AggregateCall aggrCall : aggrCallList) {
          LogicalExpression logicalExp = DrillAggregateRel.toDrill(aggrCall, aggrFields, null);
          SolrAggrQueryBuilder sAggrBuilder = new SolrAggrQueryBuilder(solrGroupScan, logicalExp);
          SolrAggrParam sggrParam = sAggrBuilder.parseTree();

          if (sggrParam != null) {
            sAggrLst.add(sggrParam);
          }

          if (sAggrBuilder.isAllExpressionsConverted()) {
            isAllExpressionConverted = true;
          } else {
            return false;
          }
        }

        return isAllExpressionConverted;
  }

  private void setScanSpecForAggregation(DrillParseContext drillParseContext,
      SolrGroupScan solrGroupScan, SolrScanSpec newScanSpec, List<SolrAggrParam> sAggrLst,
      DrillAggregateRel aggregate, DrillProjectRel project, DrillScanRel scan) {

    if (isGroupingAllowedOnField(project, sAggrLst, project.getProjects())) {

      if (!sAggrLst.isEmpty()) {
        newScanSpec.setAggrParams(sAggrLst);
        if (aggregate.getGroupCount() >= 1) {
          newScanSpec.setGroup(true);
        }
      } else if (aggregate.getGroupCount() >= 1) {
        newScanSpec.setGroup(true);
      }
      if (!newScanSpec.getAggrParams().isEmpty() || newScanSpec.isGroup()) {
        newScanSpec.setAggregateQuery(true);
        newScanSpec.setCvSchema(solrGroupScan.getSolrScanSpec().getCvSchema());
        newScanSpec.setSolrUrl(solrGroupScan.getSolrScanSpec().getSolrUrl());

        if (project != null) {
          newScanSpec.setProjectFieldNames(project.getRowType().getFieldNames());
        }
      }
    }
  }
}
