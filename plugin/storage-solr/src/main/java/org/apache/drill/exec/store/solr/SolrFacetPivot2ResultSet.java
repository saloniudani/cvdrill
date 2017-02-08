package org.apache.drill.exec.store.solr;

import java.util.List;
import java.util.Stack;

import org.apache.solr.client.solrj.response.PivotField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrFacetPivot2ResultSet extends Stack<Object> {
  /**
   *
   */
  private static final long serialVersionUID = -6609185939445669217L;
  static final Logger logger = LoggerFactory.getLogger(SolrFacetPivot2ResultSet.class);
  private boolean allLeafNodesAdded = false;
  private int leafNodesCount = 0;

  public void build() {

  }

  public void build(List<PivotField> facetPivots) {
    for (PivotField pivotField : facetPivots) {

      int pivotSize = pivotField.getPivot() != null ? pivotField.getPivot().size() : 0;
      SolrFacetPivotRecord oSolrFacetPivotRecord = new SolrFacetPivotRecord();
      oSolrFacetPivotRecord.setPivotField(pivotField);
      oSolrFacetPivotRecord.setPivotSize(pivotSize);
      this.push(oSolrFacetPivotRecord);

      if (pivotSize > 0) {
        build(pivotField.getPivot());
      }

    }
  }

  public int getLeafNodesCount() {
    return leafNodesCount;
  }

  public boolean isAllLeafNodesAdded() {
    return allLeafNodesAdded;
  }

  public void setAllLeafNodesAdded(boolean allLeafNodesAdded) {
    this.allLeafNodesAdded = allLeafNodesAdded;
  }

  public void setLeafNodesCount(int leafNodesCount) {
    this.leafNodesCount = leafNodesCount;
  }
}
