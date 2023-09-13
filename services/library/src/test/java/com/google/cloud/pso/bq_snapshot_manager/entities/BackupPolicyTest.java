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

import static org.junit.Assert.assertEquals;

import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class BackupPolicyTest {

  @Test
  public void testFromJson1() {

    String jsonPolicyStr =
        "{\n"
            + "    \"backup_cron\": \"*****\",\n"
            + "    \"backup_method\": \"BigQuery Snapshot\",\n"
            + "    \"backup_time_travel_offset_days\": \"0\",\n"
            + "    \"bq_snapshot_expiration_days\": \"15\",\n"
            + "    \"backup_storage_project\": \"storage_project\",\n"
            + "    \"backup_operation_project\": \"operation_project\",\n"
            + "    \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "    \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "    \"gcs_snapshot_format\": \"AVRO\",\n"
            + "    \"config_source\": \"SYSTEM\"\n"
            + "  }";

    BackupPolicy expected =
        new BackupPolicy.BackupPolicyBuilder(
                "*****",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.SYSTEM,
                "storage_project")
            .setBackupOperationProject("operation_project")
            .setBigQuerySnapshotExpirationDays(15.0)
            .setBigQuerySnapshotStorageDataset("dataset")
            .setGcsSnapshotStorageLocation("gs://bla/")
            .setGcsExportFormat(GCSSnapshotFormat.AVRO)
            .build();

    BackupPolicy actual = BackupPolicy.fromJson(jsonPolicyStr);

    assertEquals(expected, actual);
  }

  @Test
  public void testFromJson2() {

    String jsonPolicyStr =
        "{\n"
            + "    \"backup_cron\": \"*****\",\n"
            + "    \"backup_method\": \"BigQuery Snapshot\",\n"
            + "    \"backup_time_travel_offset_days\": \"0\",\n"
            + "    \"bq_snapshot_expiration_days\": \"15\",\n"
            + "    \"backup_storage_project\": \"storage_project\",\n"
            +
            // "    \"backup_operation_project\": \"operation_project\",\n" +
            "    \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "    \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "    \"gcs_snapshot_format\": \"AVRO\",\n"
            + "    \"config_source\": \"SYSTEM\"\n"
            + "  }";

    BackupPolicy expected =
        new BackupPolicy.BackupPolicyBuilder(
                "*****",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.SYSTEM,
                "storage_project")
            // .setBackupOperationProject("operation_project")
            .setBigQuerySnapshotExpirationDays(15.0)
            .setBigQuerySnapshotStorageDataset("dataset")
            .setGcsSnapshotStorageLocation("gs://bla/")
            .setGcsExportFormat(GCSSnapshotFormat.AVRO)
            .build();

    BackupPolicy actual = BackupPolicy.fromJson(jsonPolicyStr);

    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalid() throws IllegalArgumentException {

    String jsonPolicyStr =
        "{\n"
            + "    \"backup_cron\": \"*****\",\n"
            + "    \"backup_method\": \"INVALID METHOD\",\n"
            + "    \"backup_time_travel_offset_days\": \"0\",\n"
            + "    \"bq_snapshot_expiration_days\": \"15\",\n"
            + "    \"backup_storage_project\": \"storage_project\",\n"
            + "    \"backup_operation_project\": \"operation_project\",\n"
            + "    \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "    \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "    \"gcs_snapshot_format\": \"\",\n"
            + "    \"config_source\": \"SYSTEM\"\n"
            + "  }";

    // should fail because of backup_method = "INVALID METHOD"
    BackupPolicy.fromJson(jsonPolicyStr);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissing() throws IllegalArgumentException {

    String jsonPolicyStr =
        "{\n"
            + "    \"backup_time_travel_offset_days\": \"0\",\n"
            + "    \"bq_snapshot_expiration_days\": \"15\" \n"
            + "  }";

    // should fail because of missing configurations
    BackupPolicy.fromJson(jsonPolicyStr);
  }

  @Test
  public void testFromMapFromDCTagManualInitial() throws IOException, IllegalArgumentException {

    // data catalog manual assigned tag
    // is first run (last_xys fields are not set)
    Map<String, String> tagMap = new HashMap<>();

    tagMap.put("backup_cron", "test-cron");
    tagMap.put("backup_method", "BigQuery Snapshot");
    tagMap.put("config_source", "Manual");
    tagMap.put("backup_time_travel_offset_days", "0");
    tagMap.put("backup_storage_project", "storage-project");
    tagMap.put("backup_operation_project", "operation-project");
    tagMap.put("bq_snapshot_storage_dataset", "test-dataset");
    tagMap.put("bq_snapshot_expiration_days", "0.0");

    BackupPolicy expected =
        new BackupPolicy.BackupPolicyBuilder(
                "test-cron",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.MANUAL,
                "storage-project")
            .setBackupOperationProject("operation-project")
            .setBigQuerySnapshotExpirationDays(0.0)
            .setBigQuerySnapshotStorageDataset("test-dataset")
            .build();

    BackupPolicy actual = BackupPolicy.fromMap(tagMap);

    assertEquals(expected, actual);
  }

  @Test
  public void testFromMapFromFallbackTagInitial() throws IOException, IllegalArgumentException {

    // system assigned tags. no config_source field
    // first run (last_xys fields are not set)

    Map<String, String> tagMap = new HashMap<>();

    tagMap.put("backup_cron", "test-cron");
    tagMap.put("backup_method", "BigQuery Snapshot");
    tagMap.put("backup_time_travel_offset_days", "0");
    tagMap.put("backup_storage_project", "storage-project");
    tagMap.put("backup_operation_project", "operation-project");
    tagMap.put("bq_snapshot_storage_dataset", "test-dataset");
    tagMap.put("bq_snapshot_expiration_days", "0.0");

    BackupPolicy expected =
        new BackupPolicy.BackupPolicyBuilder(
                "test-cron",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.SYSTEM,
                "storage-project")
            .setBackupOperationProject("operation-project")
            .setBigQuerySnapshotExpirationDays(0.0)
            .setBigQuerySnapshotStorageDataset("test-dataset")
            .build();

    BackupPolicy actual = BackupPolicy.fromMap(tagMap);

    assertEquals(expected, actual);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFromMapFromFallbackFalse_exception() {

    Map<String, String> tagMap = new HashMap<>();

    // 3 missing fields (last_backup, last_bq, last_gcs) should throw an exception
    tagMap.put("backup_cron", "test-cron");
    tagMap.put("backup_method", "BigQuery Snapshot");
    tagMap.put("config_source", "System");
    tagMap.put("backup_time_travel_offset_days", "0");
    tagMap.put("backup_storage_project", "storage-project");
    tagMap.put("backup_operation_project", "operation-project");
    tagMap.put("bq_snapshot_storage_dataset", "test-dataset");
    tagMap.put("bq_snapshot_expiration_days", "0.0");
    tagMap.put("gcs_snapshot_storage_location", "test-bucket");
    tagMap.put("gcs_snapshot_format", "");

    BackupPolicy expected =
        new BackupPolicy.BackupPolicyBuilder(
                "test-cron",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.SYSTEM,
                "storage-project")
            .setBackupOperationProject("operation_project")
            .setBigQuerySnapshotExpirationDays(0.0)
            .setBigQuerySnapshotStorageDataset("test-dataset")
            .setGcsSnapshotStorageLocation("test-bucket")
            .build();

    BackupPolicy actual = BackupPolicy.fromMap(tagMap);

    assertEquals(expected, actual);
  }

  @Test // (expected = IllegalArgumentException.class)
  public void testMissingValues() {

    Map<String, String> tagMap = new HashMap<>();

    // 3 missing fields (last_backup, last_bq, last_gcs) should throw an exception
    tagMap.put("backup_cron", "test-cron");
    tagMap.put("backup_method", "BigQuery Snapshot");
    tagMap.put("bq_snapshot_storage_dataset", "dataset");
    tagMap.put("bq_snapshot_expiration_days", "1");
    tagMap.put("config_source", "System");
    tagMap.put("backup_time_travel_offset_days", "0");
    tagMap.put("backup_storage_project", "project");
    tagMap.put("backup_operation_project", "project");
    // missing backup_project and bq_snapshot_storage_dataset and bq_snapshot_expiration_days

    BackupPolicy.fromMap(tagMap);
  }

  @Test
  public void testValidate_Required() {

    BackupPolicy.BackupPolicyBuilder builder =
        new BackupPolicy.BackupPolicyBuilder(null, null, null, null, null);
    List<BackupPolicyFields> actual = BackupPolicy.validate(builder);
    List<BackupPolicyFields> expected =
        Arrays.asList(
            BackupPolicyFields.backup_cron,
            BackupPolicyFields.backup_method,
            BackupPolicyFields.backup_time_travel_offset_days,
            BackupPolicyFields.config_source,
            BackupPolicyFields.backup_storage_project);

    assertEquals(expected, actual);
  }

  @Test
  public void testValidate_BQSnapshot() {

    BackupPolicy.BackupPolicyBuilder builder =
        new BackupPolicy.BackupPolicyBuilder(
            "",
            BackupMethod.BIGQUERY_SNAPSHOT,
            TimeTravelOffsetDays.DAYS_0,
            BackupConfigSource.SYSTEM,
            "");
    List<BackupPolicyFields> actual = BackupPolicy.validate(builder);
    List<BackupPolicyFields> expected =
        Arrays.asList(
            BackupPolicyFields.bq_snapshot_storage_dataset,
            BackupPolicyFields.bq_snapshot_expiration_days);

    assertEquals(expected, actual);
  }

  @Test
  public void testValidate_GCSSnapshot() {

    BackupPolicy.BackupPolicyBuilder builder =
        new BackupPolicy.BackupPolicyBuilder(
            "",
            BackupMethod.GCS_SNAPSHOT,
            TimeTravelOffsetDays.DAYS_0,
            BackupConfigSource.SYSTEM,
            "");

    List<BackupPolicyFields> actual = BackupPolicy.validate(builder);
    List<BackupPolicyFields> expected =
        Arrays.asList(
            BackupPolicyFields.gcs_snapshot_format,
            BackupPolicyFields.gcs_snapshot_storage_location);

    assertEquals(expected, actual);
  }

  @Test
  public void testValidate_GCSSnapshotCSV() {

    BackupPolicy.BackupPolicyBuilder builder =
        new BackupPolicy.BackupPolicyBuilder(
            "",
            BackupMethod.GCS_SNAPSHOT,
            TimeTravelOffsetDays.DAYS_0,
            BackupConfigSource.SYSTEM,
            "");
    builder.setGcsExportFormat(GCSSnapshotFormat.CSV);

    List<BackupPolicyFields> actual = BackupPolicy.validate(builder);
    List<BackupPolicyFields> expected =
        Arrays.asList(
            BackupPolicyFields.gcs_snapshot_storage_location,
            BackupPolicyFields.gcs_csv_delimiter,
            BackupPolicyFields.gcs_csv_export_header);

    assertEquals(expected, actual);
  }

  @Test
  public void testValidate_GCSSnapshotAvro() {

    BackupPolicy.BackupPolicyBuilder builder =
        new BackupPolicy.BackupPolicyBuilder(
            "",
            BackupMethod.GCS_SNAPSHOT,
            TimeTravelOffsetDays.DAYS_0,
            BackupConfigSource.SYSTEM,
            "");
    builder.setGcsExportFormat(GCSSnapshotFormat.AVRO_SNAPPY);

    List<BackupPolicyFields> actual = BackupPolicy.validate(builder);
    List<BackupPolicyFields> expected =
        Arrays.asList(
            BackupPolicyFields.gcs_snapshot_storage_location,
            BackupPolicyFields.gcs_avro_use_logical_types);

    assertEquals(expected, actual);
  }
}
