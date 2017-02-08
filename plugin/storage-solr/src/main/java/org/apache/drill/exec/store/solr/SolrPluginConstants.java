package org.apache.drill.exec.store.solr;

import org.apache.solr.common.params.CursorMarkParams;

public interface SolrPluginConstants {
  public static final String DRILL_AGGREGATE_EXPR0 = "EXPR$0";
  public static final String DRILL_AGGREGATE_FIELD_EXPR0 = "$f0";
  public static final String DRILL_AGGREGATE_FIELD_EXPR1 = "$f1";
  public static final Integer SOLR_DEFAULT_FETCH_COUNT = 10 * 10 * 10 * 10 * 10;
  public static final String SOLR_DEFAULT_CURSOR_MARK = CursorMarkParams.CURSOR_MARK_START;
  public static final String CURRENT_SOLR_VERSION = "6.1";
}
