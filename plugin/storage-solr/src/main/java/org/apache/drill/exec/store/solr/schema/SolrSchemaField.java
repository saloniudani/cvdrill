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

import com.fasterxml.jackson.annotation.JsonIgnore;


public class SolrSchemaField {

  private String name;
  private String type;
  private boolean docValues = true;
  @JsonIgnore
  private boolean indexed;
  @JsonIgnore
  private boolean stored;
  @JsonIgnore
  private boolean multiValued;
  @JsonIgnore
  private boolean termVectors;
  @JsonIgnore
  private boolean omitPositions;
  @JsonIgnore
  private boolean omitNorms;
  @JsonIgnore
  private boolean required;

  @JsonIgnore
  private boolean omitTermFreqAndPositions;

  public SolrSchemaField() {
    indexed = stored = false;
  }

  public String getFieldName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public boolean isDocValues() {
    return docValues;
  }

  public boolean isIndexed() {
    return indexed;
  }

  public boolean isMultiValued() {
    return multiValued;
  }

  public boolean isOmitNorms() {
    return omitNorms;
  }

  public boolean isOmitPositions() {
    return omitPositions;
  }

  public boolean isOmitTermFreqAndPositions() {
    return omitTermFreqAndPositions;
  }

  public boolean isRequired() {
    return required;
  }


  public boolean isStored() {
    return stored;
  }

  public boolean isTermVectors() {
    return termVectors;
  }

  public void setDocValues(boolean docValues) {
    this.docValues = docValues;
  }

  public void setIndexed(boolean indexed) {
    this.indexed = indexed;
  }

  public void setMultiValued(boolean multiValued) {
    this.multiValued = multiValued;
  }

  public void setName(String fieldName) {
    this.name = fieldName;
  }

  public void setOmitNorms(boolean omitNorms) {
    this.omitNorms = omitNorms;
  }

  public void setOmitPositions(boolean omitPositions) {
    this.omitPositions = omitPositions;
  }

  public void setOmitTermFreqAndPositions(boolean omitTermFreqAndPositions) {
    this.omitTermFreqAndPositions = omitTermFreqAndPositions;
  }

  public void setRequired(boolean required) {
    this.required = required;
  }

  public void setStored(boolean stored) {
    this.stored = stored;
  }

  public void setTermVectors(boolean termVectors) {
    this.termVectors = termVectors;
  }

  public void setType(String type) {
    this.type = type;
  }
}
