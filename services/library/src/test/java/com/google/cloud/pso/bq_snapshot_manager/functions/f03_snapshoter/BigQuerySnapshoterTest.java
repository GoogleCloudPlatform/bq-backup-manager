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

import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.RetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.*;
import com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger.TaggerRequest;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import com.google.cloud.pso.bq_snapshot_manager.services.PersistentSetTestImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.PubSubServiceTestImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.bq.BigQueryService;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.Test;

public class BigQuerySnapshoterTest {

  @Test
  public void testGetSnapshotTableSpec() {

    TableSpec actual =
        BigQuerySnapshoter.getSnapshotTableSpec(
            TableSpec.fromSqlString("p.d.t"), "sProject", "sDataset", "123", 100L);

    TableSpec expected = new TableSpec("sProject", "sDataset", "p_d_t_123_100");

    assertEquals(expected, actual);
  }

  @Test
  public void testExecute()
      throws NonRetryableApplicationException, IOException, InterruptedException,
          RetryableApplicationException {

    BigQuerySnapshoter snapshoter =
        new BigQuerySnapshoter(
            new SnapshoterConfig("host-project", "data-region", "bq_backup_manager"),
            new BigQueryService() {
              @Override
              public void createSnapshot(
                  String jobId,
                  TableSpec sourceTable,
                  TableSpec destinationId,
                  Timestamp snapshotExpirationTs,
                  String trackingId)
                  throws InterruptedException {}

              @Override
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
                  throws InterruptedException {}

              @Override
              public Long getTableCreationTime(TableSpec table) {
                return 0L;
              }
            },
            new PubSubServiceTestImpl(),
            new PersistentSetTestImpl(),
            "test-prefix",
            3);

    BackupPolicy backupPolicy =
        new BackupPolicy.BackupPolicyBuilder(
                "test-cron",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_3,
                BackupConfigSource.SYSTEM,
                "backup-p")
            .setBackupOperationProject("backup-p")
            .setBigQuerySnapshotExpirationDays(15.0)
            .setBigQuerySnapshotStorageDataset("backup-d")
            .setGcsSnapshotStorageLocation("gs://bucket/folder")
            .build();

    TableSpec sourceTable = TableSpec.fromSqlString("project.dataset.table");
    Timestamp operationTime = Timestamp.ofTimeSecondsAndNanos(1667478075L, 0);
    Long timeTravelMilis = (Utils.timestampToUnixTimeMillis(operationTime) - (3 * 86400000));
    TableSpec expectedSourceTable =
        TableSpec.fromSqlString("project.dataset.table@" + timeTravelMilis);
    TableSpec expectedSnapshotTable =
        TableSpec.fromSqlString("backup-p.backup-d.project_dataset_table_runId_" + timeTravelMilis);

    BigQuerySnapshoterResponse actualResponse =
        snapshoter.execute(
            new SnapshoterRequest(
                sourceTable,
                "runId",
                "trackingId",
                false,
                new BackupPolicyAndState(backupPolicy, null)),
            operationTime,
            "pubsub-message-id");

    TaggerRequest expectedTaggerRequest =
        new TaggerRequest(
            sourceTable,
            "runId",
            "trackingId",
            false,
            new BackupPolicyAndState(backupPolicy, null),
            BackupMethod.BIGQUERY_SNAPSHOT,
            expectedSnapshotTable,
            null,
            operationTime);

    assertEquals(expectedTaggerRequest, actualResponse.getOutputTaggerRequest());
    assertEquals(expectedSourceTable, actualResponse.getComputedSourceTable());
    assertEquals(expectedSnapshotTable, actualResponse.getComputedSnapshotTable());
    assertEquals(operationTime, actualResponse.getOperationTs());
  }
}
