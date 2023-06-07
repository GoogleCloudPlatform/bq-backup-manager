/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.pso.bq_snapshot_manager.entities;

import com.google.cloud.Timestamp;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.*;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BackupPolicyTest {

    @Test
    public void testFromJson(){

        String jsonPolicyStr = "{\n" +
                "    \"backup_cron\": \"*****\",\n" +
                "    \"backup_method\": \"BigQuery Snapshot\",\n" +
                "    \"backup_time_travel_offset_days\": \"0\",\n" +
                "    \"bq_snapshot_expiration_days\": \"15\",\n" +
                "    \"backup_project\": \"project\",\n" +
                "    \"bq_snapshot_storage_dataset\": \"dataset\",\n" +
                "    \"gcs_snapshot_storage_location\": \"gs://bla/\",\n" +
                "    \"gcs_snapshot_format\": \"AVRO\",\n" +
                "    \"config_source\": \"SYSTEM\"\n" +
                "  }";

        BackupPolicy expected = new BackupPolicy.BackupPolicyBuilder("*****",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.SYSTEM,
                "project")
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

        String jsonPolicyStr = "{\n" +
                "    \"backup_cron\": \"*****\",\n" +
                "    \"backup_method\": \"INVALID METHOD\",\n" +
                "    \"backup_time_travel_offset_days\": \"0\",\n" +
                "    \"bq_snapshot_expiration_days\": \"15\",\n" +
                "    \"backup_project\": \"project\",\n" +
                "    \"bq_snapshot_storage_dataset\": \"dataset\",\n" +
                "    \"gcs_snapshot_storage_location\": \"gs://bla/\",\n" +
                "    \"gcs_snapshot_format\": \"\",\n" +
                "    \"config_source\": \"SYSTEM\"\n" +
                "  }";

        // should fail because of backup_method = "INVALID METHOD"
        BackupPolicy.fromJson(jsonPolicyStr);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissing() throws IllegalArgumentException {

        String jsonPolicyStr = "{\n" +
                "    \"backup_time_travel_offset_days\": \"0\",\n" +
                "    \"bq_snapshot_expiration_days\": \"15\" \n" +
                "  }";

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
        tagMap.put("backup_project", "test-project");
        tagMap.put("bq_snapshot_storage_dataset", "test-dataset");
        tagMap.put("bq_snapshot_expiration_days", "0.0");

        BackupPolicy expected = new BackupPolicy.BackupPolicyBuilder("test-cron",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.MANUAL,
                "test-project")
                .setBigQuerySnapshotExpirationDays(0.0)
                .setBigQuerySnapshotStorageDataset("test-dataset")
                .build();

        BackupPolicy actual = BackupPolicy.fromMap(tagMap);

        assertEquals(expected, actual);
    }

    @Test
    public void testFromMapFromDCTagManualSubsequent() throws IOException, IllegalArgumentException {

        // data catalog manual assigned tag
        // NOT first run (last_xys fields are set)

        Map<String, String> tagMap = new HashMap<>();

        tagMap.put("backup_cron", "test-cron");
        tagMap.put("backup_method", "BigQuery Snapshot");
        tagMap.put("config_source", "Manual");
        tagMap.put("backup_time_travel_offset_days", "0");
        tagMap.put("backup_project", "test-project");
        tagMap.put("bq_snapshot_storage_dataset", "test-dataset");
        tagMap.put("bq_snapshot_expiration_days", "0.0");
        tagMap.put("last_backup_at", Timestamp.MAX_VALUE.toString());
        tagMap.put("last_bq_snapshot_storage_uri", "last bq uri");

        BackupPolicy expected = new BackupPolicy.BackupPolicyBuilder("test-cron",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.MANUAL,
                "test-project")
                .setBigQuerySnapshotExpirationDays(0.0)
                .setBigQuerySnapshotStorageDataset("test-dataset")
                .setLastBqSnapshotStorageUri("last bq uri")
                .setLastBackupAt(Timestamp.MAX_VALUE)
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
        tagMap.put("backup_project", "test-project");
        tagMap.put("bq_snapshot_storage_dataset", "test-dataset");
        tagMap.put("bq_snapshot_expiration_days", "0.0");

        BackupPolicy expected = new BackupPolicy.BackupPolicyBuilder("test-cron",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.SYSTEM,
                "test-project")
                .setBigQuerySnapshotExpirationDays(0.0)
                .setBigQuerySnapshotStorageDataset("test-dataset")
                .build();

        BackupPolicy actual = BackupPolicy.fromMap(tagMap);

        assertEquals(expected, actual);
    }

    @Test
    public void testFromMapFromFallbackTagManualSubsequent() throws IOException, IllegalArgumentException {

        // system assigned tags. no config_source field
        // NOT first run (last_xys fields are set)

        Map<String, String> tagMap = new HashMap<>();

        tagMap.put("backup_cron", "test-cron");
        tagMap.put("backup_method", "BigQuery Snapshot");
        tagMap.put("backup_time_travel_offset_days", "0");
        tagMap.put("backup_project", "test-project");
        tagMap.put("bq_snapshot_storage_dataset", "test-dataset");
        tagMap.put("bq_snapshot_expiration_days", "0.0");
        tagMap.put("last_backup_at", Timestamp.MAX_VALUE.toString());
        tagMap.put("last_bq_snapshot_storage_uri", "last bq uri");

        BackupPolicy expected = new BackupPolicy.BackupPolicyBuilder("test-cron",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.SYSTEM,
                "test-project")
                .setBigQuerySnapshotExpirationDays(0.0)
                .setBigQuerySnapshotStorageDataset("test-dataset")
                .setLastBackupAt(Timestamp.MAX_VALUE)
                .setLastBqSnapshotStorageUri("last bq uri")
                .build();

        BackupPolicy actual = BackupPolicy.fromMap(tagMap);

        assertEquals(expected, actual);
    }



    @Test(expected = IllegalArgumentException.class)
    public void testFromMapFromFallbackFalse_exception()  {

        Map<String, String> tagMap = new HashMap<>();

        //3 missing fields (last_backup, last_bq, last_gcs) should throw an exception
        tagMap.put("backup_cron", "test-cron");
        tagMap.put("backup_method", "BigQuery Snapshot");
        tagMap.put("config_source", "System");
        tagMap.put("backup_time_travel_offset_days", "0");
        tagMap.put("backup_project", "test-project");
        tagMap.put("bq_snapshot_storage_dataset", "test-dataset");
        tagMap.put("bq_snapshot_expiration_days", "0.0");
        tagMap.put("gcs_snapshot_storage_location", "test-bucket");
        tagMap.put("gcs_snapshot_format", "");

        BackupPolicy expected = new BackupPolicy.BackupPolicyBuilder("test-cron",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.SYSTEM,
                "test-project")
                .setBigQuerySnapshotExpirationDays(0.0)
                .setBigQuerySnapshotStorageDataset("test-dataset")
                .setGcsSnapshotStorageLocation("test-bucket")
                .setLastBackupAt(Timestamp.MAX_VALUE)
                .setLastBqSnapshotStorageUri("last bq uri")
                .setLastGcsSnapshotStorageUri("last gcs uri")
                .build();

        BackupPolicy actual = BackupPolicy.fromMap(tagMap);

        assertEquals(expected, actual);
    }

    @Test //(expected = IllegalArgumentException.class)
    public void testMissingValues()  {

        Map<String, String> tagMap = new HashMap<>();

        //3 missing fields (last_backup, last_bq, last_gcs) should throw an exception
        tagMap.put("backup_cron", "test-cron");
        tagMap.put("backup_method", "BigQuery Snapshot");
        tagMap.put("bq_snapshot_storage_dataset", "dataset");
        tagMap.put("bq_snapshot_expiration_days", "1");
        tagMap.put("config_source", "System");
        tagMap.put("backup_time_travel_offset_days", "0");
        tagMap.put("backup_project", "project");
        // missing backup_project and bq_snapshot_storage_dataset and bq_snapshot_expiration_days

        BackupPolicy.fromMap(tagMap);
    }

    @Test
    public void testValidate_Required(){

        BackupPolicy.BackupPolicyBuilder builder = new BackupPolicy.BackupPolicyBuilder(null,null,null,null,null);
        List<DataCatalogBackupPolicyTagFields> actual = BackupPolicy.validate(builder);
        List<DataCatalogBackupPolicyTagFields> expected = Arrays.asList(
                DataCatalogBackupPolicyTagFields.backup_cron,
                DataCatalogBackupPolicyTagFields.backup_method,
                DataCatalogBackupPolicyTagFields.backup_time_travel_offset_days,
                DataCatalogBackupPolicyTagFields.config_source,
                DataCatalogBackupPolicyTagFields.backup_project
        );

        assertEquals(expected, actual);
    }

    @Test
    public void testValidate_BQSnapshot(){

        BackupPolicy.BackupPolicyBuilder builder = new BackupPolicy.BackupPolicyBuilder("",BackupMethod.BIGQUERY_SNAPSHOT,TimeTravelOffsetDays.DAYS_0,BackupConfigSource.SYSTEM,"");
        List<DataCatalogBackupPolicyTagFields> actual = BackupPolicy.validate(builder);
        List<DataCatalogBackupPolicyTagFields> expected = Arrays.asList(
                DataCatalogBackupPolicyTagFields.bq_snapshot_storage_dataset,
                DataCatalogBackupPolicyTagFields.bq_snapshot_expiration_days
        );

        assertEquals(expected, actual);
    }

    @Test
    public void testValidate_GCSSnapshot(){

        BackupPolicy.BackupPolicyBuilder builder = new BackupPolicy.BackupPolicyBuilder("",BackupMethod.GCS_SNAPSHOT,TimeTravelOffsetDays.DAYS_0,BackupConfigSource.SYSTEM,"");

        List<DataCatalogBackupPolicyTagFields> actual = BackupPolicy.validate(builder);
        List<DataCatalogBackupPolicyTagFields> expected = Arrays.asList(
                DataCatalogBackupPolicyTagFields.gcs_snapshot_format,
                DataCatalogBackupPolicyTagFields.gcs_snapshot_storage_location
        );

        assertEquals(expected, actual);
    }

    @Test
    public void testValidate_GCSSnapshotCSV(){

        BackupPolicy.BackupPolicyBuilder builder = new BackupPolicy.BackupPolicyBuilder("",BackupMethod.GCS_SNAPSHOT,TimeTravelOffsetDays.DAYS_0,BackupConfigSource.SYSTEM,"");
        builder.setGcsExportFormat(GCSSnapshotFormat.CSV);

        List<DataCatalogBackupPolicyTagFields> actual = BackupPolicy.validate(builder);
        List<DataCatalogBackupPolicyTagFields> expected = Arrays.asList(
                DataCatalogBackupPolicyTagFields.gcs_snapshot_storage_location,
                DataCatalogBackupPolicyTagFields.gcs_csv_delimiter,
                DataCatalogBackupPolicyTagFields.gcs_csv_export_header
        );

        assertEquals(expected, actual);
    }

    @Test
    public void testValidate_GCSSnapshotAvro(){

        BackupPolicy.BackupPolicyBuilder builder = new BackupPolicy.BackupPolicyBuilder("",BackupMethod.GCS_SNAPSHOT,TimeTravelOffsetDays.DAYS_0,BackupConfigSource.SYSTEM,"");
        builder.setGcsExportFormat(GCSSnapshotFormat.AVRO_SNAPPY);

        List<DataCatalogBackupPolicyTagFields> actual = BackupPolicy.validate(builder);
        List<DataCatalogBackupPolicyTagFields> expected = Arrays.asList(
                DataCatalogBackupPolicyTagFields.gcs_snapshot_storage_location,
                DataCatalogBackupPolicyTagFields.gcs_avro_use_logical_types
        );

        assertEquals(expected, actual);
    }
}
