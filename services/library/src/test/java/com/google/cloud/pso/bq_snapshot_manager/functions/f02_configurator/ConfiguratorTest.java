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
package com.google.cloud.pso.bq_snapshot_manager.functions.f02_configurator;

import com.google.cloud.Timestamp;
import com.google.cloud.Tuple;
import com.google.cloud.datacatalog.v1.Tag;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.GCSSnapshotFormat;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.*;
import com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter.SnapshoterRequest;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.TrackingHelper;
import com.google.cloud.pso.bq_snapshot_manager.services.PersistentSetTestImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.PubSubServiceTestImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.catalog.DataCatalogService;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubPublishResults;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Time;
import java.time.LocalDateTime;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class ConfiguratorTest {

    LoggingHelper testLogger = new LoggingHelper(
            ConfiguratorTest.class.getSimpleName(),
            2,
            "testProject"
    );

    String jsonPolicyStr = "{\n" +
            "  \"default_policy\": {\n" +
            "    \"backup_cron\": \"* * * * * *\",\n" +
            "    \"backup_method\": \"BigQuery Snapshot\",\n" +
            "    \"backup_time_travel_offset_days\": \"0\",\n" +
            "    \"bq_snapshot_expiration_days\": \"15\",\n" +
            "    \"backup_project\": \"project\",\n" +
            "    \"bq_snapshot_storage_dataset\": \"dataset\"\n" +
            "  },\n" +
            "  \"folder_overrides\": {\n" +
            "    \"folder1\": {\n" +
            "      \"backup_cron\": \"*****\",\n" +
            "      \"backup_method\": \"BigQuery Snapshot\",\n" +
            "      \"backup_time_travel_offset_days\": \"0\",\n" +
            "      \"bq_snapshot_expiration_days\": \"15\",\n" +
            "      \"backup_project\": \"project\",\n" +
            "      \"bq_snapshot_storage_dataset\": \"dataset\",\n" +
            "      \"gcs_snapshot_storage_location\": \"gs://bla/\"\n" +
            "    },\n" +
            "    \"folder2\": {\n" +
            "      \"backup_cron\": \"*****\",\n" +
            "      \"backup_method\": \"BigQuery Snapshot\",\n" +
            "      \"backup_time_travel_offset_days\": \"0\",\n" +
            "      \"bq_snapshot_expiration_days\": \"15\",\n" +
            "      \"backup_project\": \"project\",\n" +
            "      \"bq_snapshot_storage_dataset\": \"dataset\",\n" +
            "      \"gcs_snapshot_storage_location\": \"gs://bla/\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"project_overrides\": {\n" +
            "    \"project1\": {\n" +
            "      \"backup_cron\": \"*****\",\n" +
            "      \"backup_method\": \"BigQuery Snapshot\",\n" +
            "      \"backup_time_travel_offset_days\": \"0\",\n" +
            "      \"bq_snapshot_expiration_days\": \"15\",\n" +
            "      \"backup_project\": \"project\",\n" +
            "      \"bq_snapshot_storage_dataset\": \"dataset\",\n" +
            "      \"gcs_snapshot_storage_location\": \"gs://bla/\"\n" +
            "    },\n" +
            "    \"project2\": {\n" +
            "      \"backup_cron\": \"*****\",\n" +
            "      \"backup_method\": \"BigQuery Snapshot\",\n" +
            "      \"backup_time_travel_offset_days\": \"0\",\n" +
            "      \"bq_snapshot_expiration_days\": \"15\",\n" +
            "      \"backup_project\": \"project\",\n" +
            "      \"bq_snapshot_storage_dataset\": \"dataset\",\n" +
            "      \"gcs_snapshot_storage_location\": \"gs://bla/\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"dataset_overrides\": {\n" +
            "    \"dataset1\": {\n" +
            "      \"backup_cron\": \"*****\",\n" +
            "      \"backup_method\": \"BigQuery Snapshot\",\n" +
            "      \"backup_time_travel_offset_days\": \"0\",\n" +
            "      \"bq_snapshot_expiration_days\": \"15\",\n" +
            "      \"backup_project\": \"project\",\n" +
            "      \"bq_snapshot_storage_dataset\": \"dataset\",\n" +
            "      \"gcs_snapshot_storage_location\": \"gs://bla/\"\n" +
            "    },\n" +
            "    \"dataset2\": {\n" +
            "      \"backup_cron\": \"*****\",\n" +
            "      \"backup_method\": \"BigQuery Snapshot\",\n" +
            "      \"backup_time_travel_offset_days\": \"0\",\n" +
            "      \"bq_snapshot_expiration_days\": \"15\",\n" +
            "      \"backup_project\": \"project\",\n" +
            "      \"bq_snapshot_storage_dataset\": \"dataset\",\n" +
            "      \"gcs_snapshot_storage_location\": \"gs://bla/\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"table_overrides\": {\n" +
            "    \"table1\": {\n" +
            "      \"backup_cron\": \"*****\",\n" +
            "      \"backup_method\": \"BigQuery Snapshot\",\n" +
            "      \"backup_time_travel_offset_days\": \"0\",\n" +
            "      \"bq_snapshot_expiration_days\": \"15\",\n" +
            "      \"backup_project\": \"project\",\n" +
            "      \"bq_snapshot_storage_dataset\": \"dataset\",\n" +
            "      \"gcs_snapshot_storage_location\": \"gs://bla/\"\n" +
            "    },\n" +
            "    \"table2\": {\n" +
            "      \"backup_cron\": \"*****\",\n" +
            "      \"backup_method\": \"BigQuery Snapshot\",\n" +
            "      \"backup_time_travel_offset_days\": \"0\",\n" +
            "      \"bq_snapshot_expiration_days\": \"15\",\n" +
            "      \"backup_project\": \"project\",\n" +
            "      \"bq_snapshot_storage_dataset\": \"dataset\",\n" +
            "      \"gcs_snapshot_storage_location\": \"gs://bla/\"\n" +
            "    }\n" +
            "  }\n" +
            "}";

    BackupPolicy testPolicy = new BackupPolicy.BackupPolicyBuilder("*****",
            BackupMethod.BIGQUERY_SNAPSHOT,
            TimeTravelOffsetDays.DAYS_0,
            BackupConfigSource.SYSTEM,
            "project")
            .setBigQuerySnapshotExpirationDays(15.0)
            .setBigQuerySnapshotStorageDataset("dataset")
            .setGcsSnapshotStorageLocation("gs://bla")
            .setGcsExportFormat(GCSSnapshotFormat.AVRO)
            .setLastBackupAt(Timestamp.MIN_VALUE)
            .build();

    FallbackBackupPolicy fallbackBackupPolicy = new FallbackBackupPolicy(
            testPolicy,
            // folder level
            Stream.of(
                    new AbstractMap.SimpleEntry<>("folder1", testPolicy))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
            // project level
            Stream.of(
                    new AbstractMap.SimpleEntry<>("p2", testPolicy))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
            // dataset level
            Stream.of(
                    new AbstractMap.SimpleEntry<>("p1.d2", testPolicy))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
            // table level
            Stream.of(new AbstractMap.SimpleEntry<>("p1.d1.t1", testPolicy))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
    );

    @Test
    public void testFindFallbackBackupPolicy() {

        // test table level
        Tuple<String, BackupPolicy> tableLevel = Configurator.findFallbackBackupPolicy(
                fallbackBackupPolicy,
                TableSpec.fromSqlString("p1.d1.t1")
        );

        assertEquals("table", tableLevel.x());
        assertEquals(testPolicy, tableLevel.y());

        // test dataset level
        Tuple<String, BackupPolicy> datasetLevel = Configurator.findFallbackBackupPolicy(
                fallbackBackupPolicy,
                TableSpec.fromSqlString("p1.d2.t1")
        );

        assertEquals("dataset", datasetLevel.x());
        assertEquals(testPolicy, datasetLevel.y());

        // test project level
        Tuple<String, BackupPolicy> projectLevel = Configurator.findFallbackBackupPolicy(
                fallbackBackupPolicy,
                TableSpec.fromSqlString("p2.d1.t1")
        );

        assertEquals("project", projectLevel.x());
        assertEquals(testPolicy, projectLevel.y());

        // test default level
        Tuple<String, BackupPolicy> defaultLevel = Configurator.findFallbackBackupPolicy(
                fallbackBackupPolicy,
                TableSpec.fromSqlString("p9.d1.t1")
        );

        assertEquals("global", defaultLevel.x());
        assertEquals(testPolicy, defaultLevel.y());
    }

    @Test
    public void testGetCronNextTrigger() {

        Tuple<Boolean, LocalDateTime> testFalse = Configurator.getCronNextTrigger(
                "0 0 13 * * *", // daily at 1 PM
                Timestamp.parseTimestamp("2022-10-06T14:00:00Z"), // last backup
                Timestamp.parseTimestamp("2022-10-07T12:00:00Z") // now
        );

        Tuple<Boolean, LocalDateTime> testTrue = Configurator.getCronNextTrigger(
                "0 0 13 * * *", // daily at 1 PM
                Timestamp.parseTimestamp("2022-10-06T14:00:00Z"), // last backup
                Timestamp.parseTimestamp("2022-10-07T14:00:00Z") // now
        );

        assertEquals(false, testFalse.x());
        assertEquals(
                LocalDateTime.of(2022, 10, 7, 13, 0)
                , testFalse.y());

        assertEquals(true, testTrue.x());
        assertEquals(
                LocalDateTime.of(2022, 10, 7, 13, 0)
                , testFalse.y());
    }

    @Test
    public void testIsBackupTime_case1() {

        boolean actual = Configurator.isBackupTime(
                true,
                TableSpec.fromSqlString("p.d.t"),
                "******",
                TrackingHelper.parseRunIdAsTimestamp("1641034800000-T"),
                BackupConfigSource.SYSTEM,
                Timestamp.MIN_VALUE,
                testLogger,
                "testTrackingId"
        );

        // true because isForceRun == true
        assertEquals(true, actual);
    }

    @Test
    public void testIsBackupTime_case2() {

        boolean actual = Configurator.isBackupTime(
                false,
                TableSpec.fromSqlString("p.d.t"),
                "******",
                TrackingHelper.parseRunIdAsTimestamp("1641034800000-T"),
                BackupConfigSource.SYSTEM,
                null,
                testLogger,
                "testTrackingId"
        );

        // true because source = SYSTEM & lastBackup = MIN_VALUE
        assertEquals(true, actual);
    }

    @Test
    public void testIsBackupTime_case3() {

        boolean actual = Configurator.isBackupTime(
                false,
                TableSpec.fromSqlString("p.d.t"),
                "******",
                TrackingHelper.parseRunIdAsTimestamp("1641034800000-T"),
                BackupConfigSource.MANUAL,
                null,
                testLogger,
                "testTrackingId"
        );

        // true because source = MANUAL & lastBackup = MIN_VALUE
        assertEquals(true, actual);
    }

    @Test
    public void testIsBackupTime_case4_true() {

        boolean actual = Configurator.isBackupTime(
                false,
                TableSpec.fromSqlString("p.d.t"),
                "0 0 13 * * *", // daily at 1 PM
                TrackingHelper.parseRunIdAsTimestamp("1665064800000-T"), // 2022-10-06 14:00:00
                BackupConfigSource.MANUAL,
                Timestamp.parseTimestamp("2022-10-06T12:00:00Z"),
                testLogger,
                "testTrackingId"
        );

        // true because lastBackup != MIN_VALUE and ref point > next cron trigger
        assertEquals(true, actual);
    }

    @Test
    public void testIsBackupTime_case4_false() {

        boolean actual = Configurator.isBackupTime(
                false,
                TableSpec.fromSqlString("p.d.t"),
                "0 0 13 * * *", // daily at 1 PM
                TrackingHelper.parseRunIdAsTimestamp("1665064800000-T"), // 2022-10-06 14:00:00
                BackupConfigSource.MANUAL,
                Timestamp.parseTimestamp("2022-10-06T15:00:00Z"),
                testLogger,
                "testTrackingId"
        );

        // true because lastBackup != MIN_VALUE and ref point < next cron trigger
        assertEquals(false, actual);
    }

    private ConfiguratorResponse executeConfigurator(
            TableSpec targetTable,
            String runId,
            String trackingId,
            BackupPolicy testBackupPolicy) throws NonRetryableApplicationException, InterruptedException, IOException {

        ConfiguratorConfig config = new ConfiguratorConfig(
                "test-project",
                "test-bqSnapshoterTopic",
                "test-gcsSnapshoterTopic",
                "test-templateId"
        );

        Configurator configurator = new Configurator(
                config,
                new DataCatalogService() {

                    @Override
                    public Tag createOrUpdateBackupPolicyTag(TableSpec tableSpec, BackupPolicy backupPolicy, String backupPolicyTagTemplateId) {

                        return null;
                    }

                    @Override
                    public @Nullable BackupPolicy getBackupPolicyTag(TableSpec tableSpec, String backupPolicyTagTemplateId) throws IOException, IllegalArgumentException {
                        return testBackupPolicy;
                    }
                },
                new PubSubServiceTestImpl(),
                new PersistentSetTestImpl(),
                FallbackBackupPolicy.fromJson(jsonPolicyStr), // has no effect since we return a static BackUpPolicy from DC stub
                "test-prefix",
                2
        );

        return configurator.execute(
                new ConfiguratorRequest(
                        targetTable,
                        runId,
                        trackingId,
                        false,
                        false
                ),
                "pubsubmessageid"
        );
    }

    @Test
    public void testConfiguratorWithBqSnapshots() throws IOException, NonRetryableApplicationException, InterruptedException {

        BackupPolicy backupPolicy = new BackupPolicy.BackupPolicyBuilder("* * * * * *",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_7,
                BackupConfigSource.MANUAL,
                "snapshotProject")
                .setBigQuerySnapshotExpirationDays(15.0)
                .setBigQuerySnapshotStorageDataset("snapshotDataset")
                .setGcsSnapshotStorageLocation("gs://bla/")
                .setGcsExportFormat(GCSSnapshotFormat.AVRO)
                .setLastBackupAt(Timestamp.MIN_VALUE)
                .build();

        TableSpec targetTable = TableSpec.fromSqlString("testProject.testDataset.testTable");

        ConfiguratorResponse configuratorResponse = executeConfigurator(
                targetTable,
                "1665734583289-T",
                "1665734583289-T-xyz",
                backupPolicy);

        // this run returns a static BackupPolicy. Assert expectations based on it
        PubSubPublishResults bigQueryPublishResults = configuratorResponse.getBigQueryBackupPublishingResults();
        PubSubPublishResults gcsQueryPublishResults = configuratorResponse.getGcsBackupPublishingResults();

        // there shouldn't be any GCS snapshot requests sent to PubSub
        assertEquals(0, gcsQueryPublishResults.getSuccessMessages().size());
        assertEquals(0, gcsQueryPublishResults.getFailedMessages().size());

        // there should be exactly one bigQuery Snapshot request sent to PubSub
        assertEquals(1, bigQueryPublishResults.getSuccessMessages().size());

        SnapshoterRequest actualSnapshoterRequest = (SnapshoterRequest) bigQueryPublishResults
                .getSuccessMessages()
                .get(0)
                .getMsg();

        SnapshoterRequest expectedSnapshoterRequest = new SnapshoterRequest(
                targetTable,
                "1665734583289-T",
                "1665734583289-T-xyz",
                false,
                backupPolicy
        );

        assertEquals(expectedSnapshoterRequest, actualSnapshoterRequest);
    }

    @Test
    public void testConfiguratorWithGCSSnapshots() throws IOException, NonRetryableApplicationException, InterruptedException {

        BackupPolicy backupPolicy = new BackupPolicy.BackupPolicyBuilder("*****",
                BackupMethod.GCS_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,
                BackupConfigSource.MANUAL,
                "snapshotProject")
                .setGcsSnapshotStorageLocation("gs://bucket/folder")
                .setGcsExportFormat(GCSSnapshotFormat.AVRO)
                .setGcsUseAvroLogicalTypes(true)
                .build();

        TableSpec targetTable = TableSpec.fromSqlString("testProject.testDataset.testTable");

        ConfiguratorResponse configuratorResponse = executeConfigurator(
                targetTable,
                "1665734583289-T",
                "1665734583289-T-xyz",
                backupPolicy);

        // this run returns a static BackupPolicy. Assert expectations based on it
        PubSubPublishResults bigQueryPublishResults = configuratorResponse.getBigQueryBackupPublishingResults();
        PubSubPublishResults gcsPublishResults = configuratorResponse.getGcsBackupPublishingResults();

        // there shouldn't be any BigQuery snapshot requests sent to PubSub
        assertEquals(0, bigQueryPublishResults.getSuccessMessages().size());
        assertEquals(0, bigQueryPublishResults.getFailedMessages().size());

        // there should be exactly one GCS Snapshot request sent to PubSub
        assertEquals(1, gcsPublishResults.getSuccessMessages().size());
        SnapshoterRequest actualGCSSnapshoterRequest = (SnapshoterRequest) gcsPublishResults
                .getSuccessMessages()
                .get(0)
                .getMsg();

        SnapshoterRequest expectedGCSSnapshoterRequest = new SnapshoterRequest(
                targetTable,
                "1665734583289-T",
                "1665734583289-T-xyz",
                false,
                backupPolicy
        );

        assertEquals(expectedGCSSnapshoterRequest, actualGCSSnapshoterRequest);
    }

    @Test
    public void testConfiguratorWithBothSnapshots() throws IOException, NonRetryableApplicationException, InterruptedException {

        BackupPolicy backupPolicy = new BackupPolicy.BackupPolicyBuilder("*****",
                BackupMethod.BOTH,
                TimeTravelOffsetDays.DAYS_7,
                BackupConfigSource.MANUAL,
                "snapshotProject")
                .setBigQuerySnapshotExpirationDays(15.0)
                .setBigQuerySnapshotStorageDataset("snapshotDataset")
                .setGcsSnapshotStorageLocation("gs://bucket/folder")
                .setGcsExportFormat(GCSSnapshotFormat.AVRO)
                .setGcsUseAvroLogicalTypes(true)
                .build();

        TableSpec targetTable = TableSpec.fromSqlString("testProject.testDataset.testTable");

        ConfiguratorResponse configuratorResponse = executeConfigurator(
                targetTable,
                "1665734583289-T",
                "1665734583289-T-xyz",
                backupPolicy);

        // this run returns a static BackupPolicy. Assert expectations based on it
        PubSubPublishResults bigQueryPublishResults = configuratorResponse.getBigQueryBackupPublishingResults();
        PubSubPublishResults gcsPublishResults = configuratorResponse.getGcsBackupPublishingResults();

        // there should one GCS Snapshot request sent to PubSub
        assertEquals(1, gcsPublishResults.getSuccessMessages().size());
        SnapshoterRequest actualGCSSnapshoterRequest = (SnapshoterRequest) gcsPublishResults
                .getSuccessMessages()
                .get(0)
                .getMsg();

        SnapshoterRequest expectedGCSSnapshoterRequest = new SnapshoterRequest(
                targetTable,
                "1665734583289-T",
                "1665734583289-T-xyz",
                false,
                backupPolicy
        );

        assertEquals(expectedGCSSnapshoterRequest, actualGCSSnapshoterRequest);

        // there should be one bigQuery Snapshot request sent to PubSub
        assertEquals(1, bigQueryPublishResults.getSuccessMessages().size());
        SnapshoterRequest actualSnapshoterRequest = (SnapshoterRequest) bigQueryPublishResults
                .getSuccessMessages()
                .get(0)
                .getMsg();

        SnapshoterRequest expectedSnapshoterRequest = new SnapshoterRequest(
                targetTable,
                "1665734583289-T",
                "1665734583289-T-xyz",
                false,
                backupPolicy
        );

        assertEquals(expectedSnapshoterRequest, actualSnapshoterRequest);
    }

    @Test
    public void testConfiguratorWithSystemBqSnapshots() throws IOException, NonRetryableApplicationException, InterruptedException {

        // This SYSTEM attached policy will be ignored except for the last_backup_at. The fallback policy
        // will be used instead to ensure that we're using latest fallbacks
        BackupPolicy backupPolicy = new BackupPolicy.BackupPolicyBuilder("* * * * * *",
                BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_7,
                BackupConfigSource.SYSTEM,
                "snapshotProject")
                .setBigQuerySnapshotExpirationDays(15.0)
                .setBigQuerySnapshotStorageDataset("snapshotDataset")
                .setLastBackupAt(Timestamp.MIN_VALUE)
                .build();

        TableSpec targetTable = TableSpec.fromSqlString("testProject.testDataset.testTable");

        ConfiguratorResponse configuratorResponse = executeConfigurator(
                targetTable,
                "1665734583289-T",
                "1665734583289-T-xyz",
                backupPolicy);

        // this run returns the fallback policy with Timestamp.MIN_VALUE from the system generated policy. Assert expectations based on it
        PubSubPublishResults bigQueryPublishResults = configuratorResponse.getBigQueryBackupPublishingResults();
        PubSubPublishResults gcsQueryPublishResults = configuratorResponse.getGcsBackupPublishingResults();

        // there shouldn't be any GCS snapshot requests sent to PubSub
        assertEquals(0, gcsQueryPublishResults.getSuccessMessages().size());
        assertEquals(0, gcsQueryPublishResults.getFailedMessages().size());

        // there should be exactly one bigQuery Snapshot request sent to PubSub
        assertEquals(1, bigQueryPublishResults.getSuccessMessages().size());

        SnapshoterRequest actualSnapshoterRequest = (SnapshoterRequest) bigQueryPublishResults
                .getSuccessMessages()
                .get(0)
                .getMsg();

        SnapshoterRequest expectedSnapshoterRequest = new SnapshoterRequest(
                targetTable,
                "1665734583289-T",
                "1665734583289-T-xyz",
                false,
                new BackupPolicy.BackupPolicyBuilder(
                        "* * * * * *",
                        BackupMethod.BIGQUERY_SNAPSHOT,
                        TimeTravelOffsetDays.DAYS_0,
                        BackupConfigSource.SYSTEM,
                        "project"
                )
                        .setBigQuerySnapshotExpirationDays(15.0)
                        .setBigQuerySnapshotStorageDataset("dataset")
                        .setLastBackupAt(Timestamp.MIN_VALUE)
                        .build()
        );



                assertEquals(expectedSnapshoterRequest, actualSnapshoterRequest);
    }



}
