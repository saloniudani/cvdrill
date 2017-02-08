package org.apache.drill.exec.store.solr;

import org.apache.solr.client.solrj.response.PivotField;

public class SolrFacetPivotRecord {
  private PivotField pivotField = null;
  private int pivotSize = 0;

  public PivotField getPivotField() {
    return pivotField;
  }

  public int getPivotSize() {
    return pivotSize;
  }

  public void setPivotField(PivotField pivotField) {
    this.pivotField = pivotField;
  }

  public void setPivotSize(int pivotSize) {
    this.pivotSize = pivotSize;
  }
}
