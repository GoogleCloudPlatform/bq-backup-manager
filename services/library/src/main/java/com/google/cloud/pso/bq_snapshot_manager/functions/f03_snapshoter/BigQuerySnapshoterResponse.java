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
import com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger.TaggerRequest;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubPublishResults;

public class BigQuerySnapshoterResponse extends TableOperationRequestResponse {

  private final Timestamp operationTs;
  private final TableSpec computedSourceTable;
  private final TableSpec computedSnapshotTable;
  private final TaggerRequest outputTaggerRequest;
  private final PubSubPublishResults pubSubPublishResults;

  public BigQuerySnapshoterResponse(
      TableSpec targetTable,
      String runId,
      String trackingId,
      boolean isDryRun,
      Timestamp operationTs,
      TableSpec computedSourceTable,
      TableSpec computedSnapshotTable,
      TaggerRequest outputTaggerRequest,
      PubSubPublishResults pubSubPublishResults) {
    super(targetTable, runId, trackingId, isDryRun);
    this.operationTs = operationTs;
    this.computedSourceTable = computedSourceTable;
    this.computedSnapshotTable = computedSnapshotTable;
    this.outputTaggerRequest = outputTaggerRequest;
    this.pubSubPublishResults = pubSubPublishResults;
  }

  public Timestamp getOperationTs() {
    return operationTs;
  }

  public TableSpec getComputedSourceTable() {
    return computedSourceTable;
  }

  public TableSpec getComputedSnapshotTable() {
    return computedSnapshotTable;
  }

  public TaggerRequest getOutputTaggerRequest() {
    return outputTaggerRequest;
  }

  public PubSubPublishResults getPubSubPublishResults() {
    return pubSubPublishResults;
  }
}
