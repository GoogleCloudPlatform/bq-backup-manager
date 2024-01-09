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

package com.google.cloud.pso.bq_snapshot_manager.entities;

import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import java.util.List;
import java.util.Objects;

public class DatasetSpec {

    private final String project;
    private final String dataset;


    public DatasetSpec(String project, String dataset) {
        this.project = project;
        this.dataset = dataset;
    }

    public String getProject() {
        return project;
    }

    public String getDataset() {
        return dataset;
    }


    public String toSqlString(){
        return String.format("%s.%s", project, dataset);
    }

    public DatasetId toTableId(){ return DatasetId.of(project, dataset); }

    // parse from "project.dataset.table" format
    public static DatasetSpec fromSqlString(String sqlDatasetId){
        List<String> targetDatasetSpecs = Utils.tokenize(sqlDatasetId, ".", true);
        return new DatasetSpec(
                targetDatasetSpecs.get(0),
                targetDatasetSpecs.get(1)
        );
    }

    // parse from "//bigquery.googleapis.com/projects/#project_name/datasets/#dataset_name"
    public static DatasetSpec fromFullResource(String fullResource){
        List<String> tokens = Utils.tokenize(fullResource, "/", true);
        return new DatasetSpec(
                tokens.get(2),
                tokens.get(4)
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatasetSpec tableSpec = (DatasetSpec) o;
        return Objects.equals(project, tableSpec.project) &&
                Objects.equals(dataset, tableSpec.dataset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(project, dataset);
    }



    @Override
    public String toString() {
        return "DatasetSpec{" +
                "project='" + project + '\'' +
                ", dataset='" + dataset + '\'' +
                '}';
    }
}
