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

package com.google.cloud.pso.bq_snapshot_manager.services.bq;

import com.google.cloud.Timestamp;
import com.google.cloud.Tuple;
import com.google.cloud.bigquery.*;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.RetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.GCSSnapshotFormat;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

public class BigQueryServiceImpl implements BigQueryService {

  private BigQuery bigQuery;

  public BigQueryServiceImpl(String projectId) throws IOException {
    bigQuery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
  }

  public void createSnapshot(
      String jobId,
      TableSpec sourceTable,
      TableSpec destinationTable,
      Timestamp snapshotExpirationTs,
      String trackingId)
      throws InterruptedException, RetryableApplicationException, NonRetryableApplicationException {
    CopyJobConfiguration copyJobConfiguration =
        CopyJobConfiguration.newBuilder(destinationTable.toTableId(), sourceTable.toTableId())
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_EMPTY)
            .setOperationType("SNAPSHOT")
            .setDestinationExpirationTime(snapshotExpirationTs.toString())
            .build();

    Job job =
        bigQuery.create(JobInfo.newBuilder(copyJobConfiguration).setJobId(JobId.of(jobId)).build());

    // wait for the job to complete
    job = job.waitFor();

    // if job finished with errors
    if (job.getStatus().getError() != null) {
      if (job.getStatus()
          .getError()
          .getMessage()
          .toLowerCase()
          .contains("caused by a transient issue")) {
        // In some cases snapshot jobs faces the below error. In such case we should retry it.
        // IMPROVE: detect the error based on a code or reason and not the error message
        /*
        An internal error occurred and the request could not be completed.
        This is usually caused by a transient issue.
        Retrying the job with back-off as described in the BigQuery SLA should solve the
        problem: https://cloud.google.com/bigquery/sla. If the error continues to occur
        please contact support at https://cloud.google.com/support.
         */

        String msg =
            String.format(
                "BigQuery Snapshot job %s for table %s failed due to a transient error. Msg: %s. Reason: %s",
                jobId,
                sourceTable.toSqlString(),
                job.getStatus().getError().getMessage(),
                job.getStatus().getError().getReason());
        throw new RetryableApplicationException(msg);
      } else {
        String msg =
            String.format(
                "BigQuery Snapshot job %s for table %s failed. Msg: %s. Reason: %s",
                jobId,
                sourceTable.toSqlString(),
                job.getStatus().getError().getMessage(),
                job.getStatus().getError().getReason());
        throw new NonRetryableApplicationException(msg);
      }
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
      Map<String, String> jobLabels)
      throws InterruptedException {

    Tuple<String, String> formatAndCompression =
        GCSSnapshotFormat.getFormatAndCompression(exportFormat);

    ExtractJobConfiguration.Builder extractConfigurationBuilder =
        ExtractJobConfiguration.newBuilder(sourceTable.toTableId(), gcsDestinationUri)
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
    bigQuery.create(
        JobInfo.newBuilder(extractConfigurationBuilder.build()).setJobId(JobId.of(jobId)).build());
  }

  @Override
  public Long getTableCreationTime(TableSpec tableSpec) throws NonRetryableApplicationException {
    Table table = bigQuery.getTable(tableSpec.toTableId());
    if (table != null) {
      return table.getCreationTime();
    } else {
      throw new NonRetryableApplicationException(
          String.format(
              "Requested table %s is not found. The table might have been deleted.",
              tableSpec.toSqlString()));
    }
  }
}
