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

import com.google.cloud.Timestamp;
import com.google.cloud.Tuple;
import com.google.cloud.bigquery.*;
import com.google.cloud.pso.bq_snapshot_manager.entities.Globals;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.GCSSnapshotFormat;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BigQueryServiceImpl implements BigQueryService {

    private BigQuery bigQuery;

    public BigQueryServiceImpl(String projectId) throws IOException {
        bigQuery = BigQueryOptions
                .newBuilder()
                .setProjectId(projectId)
                .build()
                .getService();
    }


    public void createSnapshot(String jobId, TableSpec sourceTable, TableSpec destinationTable, Timestamp snapshotExpirationTs, String trackingId) throws InterruptedException {
        CopyJobConfiguration copyJobConfiguration = CopyJobConfiguration
                .newBuilder(destinationTable.toTableId(), sourceTable.toTableId())
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_EMPTY)
                .setOperationType("SNAPSHOT")
                .setDestinationExpirationTime(snapshotExpirationTs.toString())
                .build();

        Job job = bigQuery.create(JobInfo
                .newBuilder(copyJobConfiguration)
                .setJobId(JobId.of(jobId))
                .build());

        // wait for the job to complete
        job = job.waitFor();

        // if job finished with errors
        if (job.getStatus().getError() != null) {
            throw new RuntimeException(job.getStatus().getError().toString());
        }
    }

    public void exportToGCS(
            String jobId,
            TableSpec sourceTable,
            String gcsDestinationUri,
            GCSSnapshotFormat exportFormat,
            @Nullable String csvFieldDelimiter,
            @Nullable Boolean csvPrintHeader,
            @Nullable Boolean useAvroLogicalTypes,
            String trackingId,
            Map<String, String> jobLabels
    ) throws InterruptedException {

        Tuple<String, String> formatAndCompression = GCSSnapshotFormat.getFormatAndCompression(exportFormat);

        ExtractJobConfiguration.Builder extractConfigurationBuilder = ExtractJobConfiguration
                .newBuilder(sourceTable.toTableId(), gcsDestinationUri)
                .setLabels(jobLabels)
                .setFormat(formatAndCompression.x());

        // check if compression is required
        if (formatAndCompression.y() != null) {
            extractConfigurationBuilder.setCompression(formatAndCompression.y());
        }

        // set optional fields
        if (csvFieldDelimiter != null) {
            extractConfigurationBuilder.setFieldDelimiter(csvFieldDelimiter);
        }
        if (csvPrintHeader != null) {
            extractConfigurationBuilder.setPrintHeader(csvPrintHeader);
        }
        if (useAvroLogicalTypes != null) {
            extractConfigurationBuilder.setUseAvroLogicalTypes(useAvroLogicalTypes);
        }

        // async call to create an export job
        bigQuery.create(JobInfo
                .newBuilder(extractConfigurationBuilder.build())
                .setJobId(JobId.of(jobId))
                .build());
    }
}