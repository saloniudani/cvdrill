package org.apache.drill.exec.store.solr.schema;

public class SolrSchemaWrapper {
  protected SolrSchemaResp schema;

  public SolrSchemaResp getSchema() {
    return schema;
  }

  public void setSchema(SolrSchemaResp schema) {
    this.schema = schema;
  }
}
