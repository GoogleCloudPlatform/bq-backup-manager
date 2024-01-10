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

package com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter;


import com.google.cloud.Timestamp;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableOperationRequestResponse;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;

public class GCSSnapshoterResponse extends TableOperationRequestResponse {

    private final Timestamp operationTs;
    private final TableSpec computedSourceTable;

    public GCSSnapshoterResponse(
            TableSpec targetTable,
            String runId,
            String trackingId,
            boolean isDryRun,
            Timestamp operationTs,
            TableSpec computedSourceTable) {
        super(targetTable, runId, trackingId, isDryRun);
        this.operationTs = operationTs;
        this.computedSourceTable = computedSourceTable;
    }

    public Timestamp getOperationTs() {
        return operationTs;
    }

    public TableSpec getComputedSourceTable() {
        return computedSourceTable;
    }
}
