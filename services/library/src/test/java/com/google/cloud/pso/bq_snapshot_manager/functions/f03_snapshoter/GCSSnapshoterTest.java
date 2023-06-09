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
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.*;
import com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger.TaggerRequest;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import com.google.cloud.pso.bq_snapshot_manager.services.PersistentMapTestImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.PersistentSetTestImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.PubSubServiceTestImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.bq.BigQueryService;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubPublishResults;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.SuccessPubSubMessage;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GCSSnapshoterTest {

    @Test
    public void testPrepareGcsUriForMultiFileExport() {

        assertEquals(
                "gs://backups/a/b/c/*",
                GCSSnapshoter.prepareGcsUriForMultiFileExport("gs://backups/", "a/b/c")
        );

        assertEquals(
                "gs://backups/a/b/c/*",
                GCSSnapshoter.prepareGcsUriForMultiFileExport("gs://backups", "/a/b/c/")
        );
    }

    @Test
    public void testExecute() throws NonRetryableApplicationException, IOException, InterruptedException {

        GCSSnapshoter gcsSnapshoter = new GCSSnapshoter(
                new SnapshoterConfig("host-project", "data-region", "bq_backup_manager"),
                new BigQueryService() {
                    @Override
                    public void createSnapshot(String jobId, TableSpec sourceTable, TableSpec destinationId, Timestamp snapshotExpirationTs, String trackingId) throws InterruptedException {
                    }

                    @Override
                    public void exportToGCS(String jobId, TableSpec sourceTable, String gcsDestinationUri, GCSSnapshotFormat exportFormat, @Nullable String csvFieldDelimiter, @Nullable Boolean csvPrintHeader, @Nullable Boolean useAvroLogicalTypes, String trackingId, Map<String, String> jobLabels) throws InterruptedException {
                    }

                    @Override
                    public Long getTableCreationTime(TableSpec table) {
                        return null;
                    }
                },
                new PubSubServiceTestImpl(),
                new PersistentSetTestImpl(),
                "test-set-prefix",
                new PersistentMapTestImpl(),
                "test-map-prefix",
                -3
        );

        BackupPolicy backupPolicy = new BackupPolicy.BackupPolicyBuilder("test-cron",
                BackupMethod.GCS_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_3,
                BackupConfigSource.SYSTEM,
                "project")
                .setBackupOperationProject("project")
                .setGcsSnapshotStorageLocation("gs://backups")
                .setGcsExportFormat(GCSSnapshotFormat.AVRO_SNAPPY)
                .setGcsUseAvroLogicalTypes(true)
                .build();

        TableSpec sourceTable = TableSpec.fromSqlString("project.dataset.table");
        Timestamp operationTime = Timestamp.ofTimeSecondsAndNanos(1667478075L, 0);
        Long timeTravelMilis = (Utils.timestampToUnixTimeMillis(operationTime) - (3* 86400000));
        TableSpec expectedSourceTable = TableSpec.fromSqlString("project.dataset.table@"+timeTravelMilis);

        GCSSnapshoterResponse actualResponse = gcsSnapshoter.execute(
                new SnapshoterRequest(
                        sourceTable,
                        "runId",
                        "trackingId",
                        false,
                        backupPolicy
                ),
                operationTime,
                "pubsub-message-id");


        assertEquals(expectedSourceTable, actualResponse.getComputedSourceTable());
        assertEquals(operationTime, actualResponse.getOperationTs());
    }
}
