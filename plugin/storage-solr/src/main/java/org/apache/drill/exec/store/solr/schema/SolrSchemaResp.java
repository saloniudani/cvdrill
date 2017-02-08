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
package org.apache.drill.exec.store.solr.schema;

import java.util.ArrayList;
import java.util.List;


public class SolrSchemaResp {

  protected String defaultSearchField = null;
  protected String uniqueKey = null;
  protected List<SolrSchemaField> fields = null;
  protected List<SolrSchemaField> dynamicFields = null;

  public SolrSchemaResp() {
  }

  public String getDefaultSearchField() {
    if (defaultSearchField == null) {
      this.defaultSearchField = "";
    }

    return this.defaultSearchField;
  }

  public String getDefaultSearchField(boolean init) {
    if (init) {
      return getDefaultSearchField();
    } else {
      return this.defaultSearchField;
    }
  }

  public List<SolrSchemaField> getDynSchemaFields() {
    if (dynamicFields == null) {
      this.dynamicFields = new ArrayList<SolrSchemaField>();
    }

    return this.dynamicFields;
  }

  public List<SolrSchemaField> getDynSchemaFields(boolean init) {
    if (init) {
      return getDynSchemaFields();
    } else {
      return this.dynamicFields;
    }
  }

  public List<SolrSchemaField> getSchemaFields() {
    if (fields == null) {
      this.fields = new ArrayList<SolrSchemaField>();
    }

    return this.fields;
  }

  public List<SolrSchemaField> getSchemaFields(boolean init) {
    if (init) {
      return getSchemaFields();
    } else {
      return this.fields;
    }
  }

  public String getUniqueKey() {
    if (uniqueKey == null) {
      this.uniqueKey = "";
    }

    return this.uniqueKey;
  }

  public String getUniqueKey(boolean init) {
    if (init) {
      return getUniqueKey();
    } else {
      return this.uniqueKey;
    }
  }

  public void setDefaultSearchField(String defaultSearchField) {
    this.defaultSearchField = defaultSearchField;
  }

  public void setDynamicFields(List<SolrSchemaField> dynamicFields) {
    this.dynamicFields = dynamicFields;
  }

  public void setFields(List<SolrSchemaField> fields) {
    this.fields = fields;
  }

  public void setUniqueKey(String uniqueKey) {
    this.uniqueKey = uniqueKey;
  }
}
