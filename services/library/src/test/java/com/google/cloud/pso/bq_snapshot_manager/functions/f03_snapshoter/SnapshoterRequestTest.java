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
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.*;
import org.junit.Test;

public class SnapshoterRequestTest {

  @Test
  public void testComputeBackupOperationProject1() {

    // test with backupOperationProject set

    BackupPolicy policy =
        new BackupPolicy.BackupPolicyBuilder(
                "test-cron",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.SYSTEM,
                "storage-project")
            .setBackupOperationProject("operation-project")
            .setBigQuerySnapshotExpirationDays(0.0)
            .setBigQuerySnapshotStorageDataset("test-dataset")
            .setGcsSnapshotStorageLocation("test-bucket")
            .build();

    SnapshoterRequest request =
        new SnapshoterRequest(
            TableSpec.fromSqlString("source_project.dataset.table"),
            "run-id",
            "tracking-id",
            false,
            new BackupPolicyAndState(
                policy, new BackupState(Timestamp.MAX_VALUE, "last bq uri", null)));

    assertEquals("operation-project", request.computeBackupOperationProject());
  }

  public void testComputeBackupOperationProject2() {

    // test with backupOperationProject missing

    BackupPolicy policy =
        new BackupPolicy.BackupPolicyBuilder(
                "test-cron",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.SYSTEM,
                "storage-project")
            .setBigQuerySnapshotExpirationDays(0.0)
            .setBigQuerySnapshotStorageDataset("test-dataset")
            .setGcsSnapshotStorageLocation("test-bucket")
            .build();

    SnapshoterRequest request =
        new SnapshoterRequest(
            TableSpec.fromSqlString("source_project.dataset.table"),
            "run-id",
            "tracking-id",
            false,
            new BackupPolicyAndState(
                policy, new BackupState(Timestamp.MAX_VALUE, "last bq uri", null)));

    assertEquals("source_project", request.computeBackupOperationProject());
  }
}
