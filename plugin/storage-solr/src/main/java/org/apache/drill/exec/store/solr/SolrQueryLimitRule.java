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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.drill.exec.planner.logical.DrillLimitRel;
import org.apache.drill.exec.planner.logical.DrillProjectRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrQueryLimitRule extends StoragePluginOptimizerRule {
  static final Logger logger = LoggerFactory.getLogger(SolrQueryLimitRule.class);

  public static final StoragePluginOptimizerRule LIMIT_ON_SCAN = new SolrQueryLimitRule(
      RelOptHelper.some(DrillLimitRel.class, RelOptHelper.any(DrillScanRel.class)),
      "SolrQueryLimitRule:Limit_On_Scan") {
    protected void doOnMatch(RelOptRuleCall call, DrillLimitRel limitRel, DrillScanRel scanRel) {
      SolrGroupScan solrGroupScan = (SolrGroupScan) scanRel.getGroupScan();
      RexNode fetch = limitRel.getFetch();
      if (solrGroupScan.getSolrScanSpec() == null) {
        return;
      }
      solrGroupScan.getSolrScanSpec().setSolrDocFetchCount(Long.valueOf(fetch.toString()));
      solrGroupScan.getSolrScanSpec().setLimitApplied(true);
      call.transformTo(scanRel);
    }

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
      SolrQueryLimitRule.logger.trace("SolrQueryLimitRule :: onMatch");
      final DrillLimitRel limitRel = (DrillLimitRel) call.rel(0);
      final DrillScanRel scanRel = (DrillScanRel) call.rel(1);
      doOnMatch(call, limitRel, scanRel);
    }
  };

  public static final StoragePluginOptimizerRule LIMIT_ON_PROJECT = new SolrQueryLimitRule(
      RelOptHelper.some(DrillLimitRel.class,
          RelOptHelper.some(DrillProjectRel.class, RelOptHelper.any(DrillScanRel.class))),
      "SolrQueryLimitRule:Limit_On_Project") {
    protected void doOnMatch(RelOptRuleCall call, DrillLimitRel limitRel,
        DrillProjectRel projectRel, DrillScanRel scanRel) {

      DrillRel inputRel = projectRel != null ? projectRel : scanRel;
      SolrGroupScan solrGroupScan = (SolrGroupScan) scanRel.getGroupScan();
      RexNode fetch = limitRel.getFetch();
      if (solrGroupScan.getSolrScanSpec() == null) {
        return;
      }

      if (fetch != null && fetch.isA(SqlKind.LITERAL)) {
        RexLiteral l = (RexLiteral) fetch;
        switch (l.getTypeName()) {
        case BIGINT:
        case INTEGER:
        case DECIMAL:
          Long limit = Long.valueOf(l.getValue2().toString());
          solrGroupScan.getSolrScanSpec().setSolrDocFetchCount(limit);
          solrGroupScan.getSolrScanSpec().setLimitApplied(true);
          break;
        }
      }

      SolrGroupScan newGroupScan = new SolrGroupScan(solrGroupScan.getUserName(),
          solrGroupScan.getSolrPlugin(), solrGroupScan.getSolrScanSpec(),
          solrGroupScan.getColumns());

      DrillScanRel newScanRel = new DrillScanRel(scanRel.getCluster(), scanRel.getTraitSet().plus(
          DrillRel.DRILL_LOGICAL), scanRel.getTable(), newGroupScan, scanRel.getRowType(),
          scanRel.getColumns());

      if (projectRel != null) {
        DrillProjectRel newProjectRel = DrillProjectRel
            .create(projectRel.getCluster(), projectRel.getTraitSet(), newScanRel,
                projectRel.getProjects(), projectRel.getRowType());
        inputRel = newProjectRel;
      } else {
        inputRel = newScanRel;
      }

      call.transformTo(inputRel);
    }

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
      SolrQueryLimitRule.logger.trace("SolrQueryLimitRule :: onMatch");
      final DrillLimitRel limitRel = (DrillLimitRel) call.rel(0);
      final DrillProjectRel ProjectRel = (DrillProjectRel) call.rel(1);
      final DrillScanRel scanRel = (DrillScanRel) call.rel(2);
      doOnMatch(call, limitRel, ProjectRel, scanRel);
    }
  };

  public SolrQueryLimitRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {

  }
}
