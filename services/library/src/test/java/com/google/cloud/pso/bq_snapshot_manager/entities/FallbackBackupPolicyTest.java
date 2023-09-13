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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.*;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

public class FallbackBackupPolicyTest {

  @Test
  public void testParsing() throws JsonProcessingException {

    String jsonPolicyStr =
        "{\n"
            + "  \"default_policy\": {\n"
            + "    \"backup_cron\": \"*****\",\n"
            + "    \"backup_method\": \"BigQuery Snapshot\",\n"
            + "    \"backup_time_travel_offset_days\": \"0\",\n"
            + "    \"bq_snapshot_expiration_days\": \"15\",\n"
            + "    \"backup_storage_project\": \"storage_project\",\n"
            + "    \"backup_operation_project\": \"operation_project\",\n"
            + "    \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "    \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "      \"config_source\": \"SYSTEM\"\n"
            + "  },\n"
            + "  \"folder_overrides\": {\n"
            + "    \"folder1\": {\n"
            + "      \"backup_cron\": \"*****\",\n"
            + "      \"backup_method\": \"BigQuery Snapshot\",\n"
            + "      \"backup_time_travel_offset_days\": \"0\",\n"
            + "      \"bq_snapshot_expiration_days\": \"15\",\n"
            + "      \"backup_storage_project\": \"storage_project\",\n"
            + "      \"backup_operation_project\": \"operation_project\",\n"
            + "      \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "      \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "      \"config_source\": \"SYSTEM\"\n"
            + "    },\n"
            + "    \"folder2\": {\n"
            + "      \"backup_cron\": \"*****\",\n"
            + "      \"backup_method\": \"BigQuery Snapshot\",\n"
            + "      \"backup_time_travel_offset_days\": \"0\",\n"
            + "      \"bq_snapshot_expiration_days\": \"15\",\n"
            + "    \"backup_storage_project\": \"storage_project\",\n"
            + "    \"backup_operation_project\": \"operation_project\",\n"
            + "      \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "      \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "      \"config_source\": \"SYSTEM\"\n"
            + "    }\n"
            + "  },\n"
            + "  \"project_overrides\": {\n"
            + "    \"project1\": {\n"
            + "      \"backup_cron\": \"*****\",\n"
            + "      \"backup_method\": \"BigQuery Snapshot\",\n"
            + "      \"backup_time_travel_offset_days\": \"0\",\n"
            + "      \"bq_snapshot_expiration_days\": \"15\",\n"
            + "      \"backup_storage_project\": \"storage_project\",\n"
            + "      \"backup_operation_project\": \"operation_project\",\n"
            + "      \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "      \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "      \"config_source\": \"SYSTEM\"\n"
            + "    },\n"
            + "    \"project2\": {\n"
            + "      \"backup_cron\": \"*****\",\n"
            + "      \"backup_method\": \"BigQuery Snapshot\",\n"
            + "      \"backup_time_travel_offset_days\": \"0\",\n"
            + "      \"bq_snapshot_expiration_days\": \"15\",\n"
            + "      \"backup_storage_project\": \"storage_project\",\n"
            + "      \"backup_operation_project\": \"operation_project\",\n"
            + "      \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "      \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "      \"config_source\": \"SYSTEM\"\n"
            + "    }\n"
            + "  },\n"
            + "  \"dataset_overrides\": {\n"
            + "    \"dataset1\": {\n"
            + "      \"backup_cron\": \"*****\",\n"
            + "      \"backup_method\": \"BigQuery Snapshot\",\n"
            + "      \"backup_time_travel_offset_days\": \"0\",\n"
            + "      \"bq_snapshot_expiration_days\": \"15\",\n"
            + "      \"backup_storage_project\": \"storage_project\",\n"
            + "      \"backup_operation_project\": \"operation_project\",\n"
            + "      \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "      \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "      \"config_source\": \"SYSTEM\"\n"
            + "    },\n"
            + "    \"dataset2\": {\n"
            + "      \"backup_cron\": \"*****\",\n"
            + "      \"backup_method\": \"BigQuery Snapshot\",\n"
            + "      \"backup_time_travel_offset_days\": \"0\",\n"
            + "      \"bq_snapshot_expiration_days\": \"15\",\n"
            + "      \"backup_storage_project\": \"storage_project\",\n"
            + "      \"backup_operation_project\": \"operation_project\",\n"
            + "      \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "      \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "      \"config_source\": \"SYSTEM\"\n"
            + "    }\n"
            + "  },\n"
            + "  \"table_overrides\": {\n"
            + "    \"table1\": {\n"
            + "      \"backup_cron\": \"*****\",\n"
            + "      \"backup_method\": \"BigQuery Snapshot\",\n"
            + "      \"backup_time_travel_offset_days\": \"0\",\n"
            + "      \"bq_snapshot_expiration_days\": \"15\",\n"
            + "      \"backup_storage_project\": \"storage_project\",\n"
            + "      \"backup_operation_project\": \"operation_project\",\n"
            + "      \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "      \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "      \"config_source\": \"SYSTEM\"\n"
            + "    },\n"
            + "    \"table2\": {\n"
            + "      \"backup_cron\": \"*****\",\n"
            + "      \"backup_method\": \"BigQuery Snapshot\",\n"
            + "      \"backup_time_travel_offset_days\": \"0\",\n"
            + "      \"bq_snapshot_expiration_days\": \"15\",\n"
            + "      \"backup_storage_project\": \"storage_project\",\n"
            + "      \"backup_operation_project\": \"operation_project\",\n"
            + "      \"bq_snapshot_storage_dataset\": \"dataset\",\n"
            + "      \"gcs_snapshot_storage_location\": \"gs://bla/\",\n"
            + "      \"config_source\": \"SYSTEM\"\n"
            + "    }\n"
            + "  }\n"
            + "}";

    BackupPolicy testPolicy =
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
            .build();

    FallbackBackupPolicy expected =
        new FallbackBackupPolicy(
            testPolicy,
            Stream.of(
                    new AbstractMap.SimpleEntry<>("folder1", testPolicy),
                    new AbstractMap.SimpleEntry<>("folder2", testPolicy))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
            Stream.of(
                    new AbstractMap.SimpleEntry<>("project1", testPolicy),
                    new AbstractMap.SimpleEntry<>("project2", testPolicy))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
            Stream.of(
                    new AbstractMap.SimpleEntry<>("dataset1", testPolicy),
                    new AbstractMap.SimpleEntry<>("dataset2", testPolicy))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
            Stream.of(
                    new AbstractMap.SimpleEntry<>("table1", testPolicy),
                    new AbstractMap.SimpleEntry<>("table2", testPolicy))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    FallbackBackupPolicy actual = FallbackBackupPolicy.fromJson(jsonPolicyStr);

    assertEquals(expected, actual);
  }
}
