/*
 *
 *  * Copyright 2023 Google LLC
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.google.cloud.pso.bq_snapshot_manager.functions.f01_1_dispatcher;

import java.util.List;
import java.util.Objects;

public class BigQueryScope {

  private List<Long> folderIncludeList;
  private List<String> projectIncludeList;
  private List<String> projectExcludeList;
  private List<String> datasetIncludeList;
  private List<String> datasetExcludeList;
  private List<String> tableIncludeList;
  private List<String> tableExcludeList;

  public BigQueryScope(
      List<Long> folderIncludeList,
      List<String> projectIncludeList,
      List<String> projectExcludeList,
      List<String> datasetIncludeList,
      List<String> datasetExcludeList,
      List<String> tableIncludeList,
      List<String> tableExcludeList) {
    this.folderIncludeList = folderIncludeList;
    this.projectIncludeList = projectIncludeList;
    this.projectExcludeList = projectExcludeList;
    this.datasetIncludeList = datasetIncludeList;
    this.datasetExcludeList = datasetExcludeList;
    this.tableIncludeList = tableIncludeList;
    this.tableExcludeList = tableExcludeList;
  }

  public List<Long> getFolderIncludeList() {
    return folderIncludeList;
  }

  public List<String> getProjectExcludeList() {
    return projectExcludeList;
  }

  public List<String> getProjectIncludeList() {
    return projectIncludeList;
  }

  public List<String> getDatasetIncludeList() {
    return datasetIncludeList;
  }

  public List<String> getDatasetExcludeList() {
    return datasetExcludeList;
  }

  public List<String> getTableIncludeList() {
    return tableIncludeList;
  }

  public List<String> getTableExcludeList() {
    return tableExcludeList;
  }

  @Override
  public String toString() {
    return "BigQueryScope{"
        + "folderIncludeList="
        + folderIncludeList
        + ", projectIncludeList="
        + projectIncludeList
        + ", projectExcludeList="
        + projectExcludeList
        + ", datasetIncludeList="
        + datasetIncludeList
        + ", datasetExcludeList="
        + datasetExcludeList
        + ", tableIncludeList="
        + tableIncludeList
        + ", tableExcludeList="
        + tableExcludeList
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BigQueryScope that = (BigQueryScope) o;
    return Objects.equals(getFolderIncludeList(), that.getFolderIncludeList())
        && Objects.equals(getProjectIncludeList(), that.getProjectIncludeList())
        && Objects.equals(getProjectExcludeList(), that.getProjectExcludeList())
        && Objects.equals(getDatasetIncludeList(), that.getDatasetIncludeList())
        && Objects.equals(getDatasetExcludeList(), that.getDatasetExcludeList())
        && Objects.equals(getTableIncludeList(), that.getTableIncludeList())
        && Objects.equals(getTableExcludeList(), that.getTableExcludeList());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getFolderIncludeList(),
        getProjectIncludeList(),
        getProjectExcludeList(),
        getDatasetIncludeList(),
        getDatasetExcludeList(),
        getTableIncludeList(),
        getTableExcludeList());
  }
}
