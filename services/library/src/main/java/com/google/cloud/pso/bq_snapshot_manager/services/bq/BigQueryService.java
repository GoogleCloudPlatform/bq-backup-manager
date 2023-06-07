/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pso.bq_snapshot_manager.services.bq;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.GCSSnapshotFormat;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.TimeTravelOffsetDays;

import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public interface BigQueryService {
    void createSnapshot(
            String jobId,
            TableSpec sourceTable,
            TableSpec destinationId,
            Timestamp snapshotExpirationTs,
            String trackingId) throws InterruptedException;

    void exportToGCS(
            String jobId,
            TableSpec sourceTable,
            String gcsDestinationUri,
            GCSSnapshotFormat exportFormat,
            @Nullable String csvFieldDelimiter,
            @Nullable Boolean csvPrintHeader,
            @Nullable Boolean useAvroLogicalTypes,
            String trackingId,
            Map<String, String> jobLabels
    ) throws InterruptedException;
}
