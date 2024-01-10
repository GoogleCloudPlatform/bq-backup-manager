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

package com.google.cloud.pso.bq_snapshot_manager.functions.f01_2_dispatcher_tables;


import com.google.cloud.pso.bq_snapshot_manager.entities.DatasetSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.JsonMessage;
import java.util.List;

public class DispatcherTableRequest extends JsonMessage {

    private String runId;

    private DatasetSpec datasetSpec;

    private List<String> tablesInclusionList;
    private List<String> tablesExclusionList;

    private boolean isDryRun;

    private boolean isForceRun;

    public DispatcherTableRequest(
            String runId,
            boolean isDryRun,
            boolean isForceRun,
            DatasetSpec datasetSpec,
            List<String> tablesInclusionList,
            List<String> tablesExclusionList) {
        this.runId = runId;
        this.isDryRun = isDryRun;
        this.isForceRun = isForceRun;
        this.datasetSpec = datasetSpec;
        this.tablesInclusionList = tablesInclusionList;
        this.tablesExclusionList = tablesExclusionList;
    }

    public String getRunId() {
        return runId;
    }

    public boolean isDryRun() {
        return isDryRun;
    }

    public boolean isForceRun() {
        return isForceRun;
    }

    public List<String> getTablesInclusionList() {
        return tablesInclusionList;
    }

    public List<String> getTablesExclusionList() {
        return tablesExclusionList;
    }

    public DatasetSpec getDatasetSpec() {
        return datasetSpec;
    }

    @Override
    public String toString() {
        return "DispatcherTableRequest{"
                + "runId='"
                + runId
                + '\''
                + ", datasetSpec="
                + datasetSpec
                + ", tablesInclusionList="
                + tablesInclusionList
                + ", tablesExclusionList="
                + tablesExclusionList
                + ", isDryRun="
                + isDryRun
                + ", isForceRun="
                + isForceRun
                + "} "
                + super.toString();
    }
}
