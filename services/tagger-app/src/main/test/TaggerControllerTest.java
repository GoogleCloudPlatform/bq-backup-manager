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

import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupMethod;
import com.google.cloud.pso.bq_snapshot_manager.tagger.TaggerController;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TaggerControllerTest {

    private final String testEvent = "{\n" +
            "  \"insertId\": \"-ksjkpxe3omqc\",\n" +
            "  \"logName\": \"projects/backup-project/logs/cloudaudit.googleapis.com%2Fdata_access\",\n" +
            "  \"protoPayload\": {\n" +
            "    \"@type\": \"type.googleapis.com/google.cloud.audit.AuditLog\",\n" +
            "    \"authenticationInfo\": {\n" +
            "      \"principalEmail\": \"snapshoter-gcs@prject.iam.gserviceaccount.com\"\n" +
            "    },\n" +
            "    \"methodName\": \"jobservice.jobcompleted\",\n" +
            "    \"requestMetadata\": {\n" +
            "      \"callerIp\": \"35.203.254.111\",\n" +
            "      \"callerSuppliedUserAgent\": \"gcloud-java/2.16.1 Google-API-Java-Client/2.0.0 Google-HTTP-Java-Client/1.42.2 (gzip),gzip(gfe)\"\n" +
            "    },\n" +
            "    \"resourceName\": \"projects/project/jobs/test-job\",\n" +
            "    \"serviceData\": {\n" +
            "      \"@type\": \"type.googleapis.com/google.cloud.bigquery.logging.v1.AuditData\",\n" +
            "      \"jobCompletedEvent\": {\n" +
            "        \"eventName\": \"extract_job_completed\",\n" +
            "        \"job\": {\n" +
            "          \"jobConfiguration\": {\n" +
            "            \"extract\": {\n" +
            "              \"destinationUris\": [\n" +
            "                \"gs://bucket/project/dataset/table/tracking_id/timestamp/AVRO_SNAPPY/*\"\n" +
            "              ],\n" +
            "              \"sourceTable\": {\n" +
            "                \"datasetId\": \"test-dataset\",\n" +
            "                \"projectId\": \"test-project\",\n" +
            "                \"tableId\": \"test-table@1675701106000\"\n" +
            "              }\n" +
            "            },\n" +
            "            \"labels\": {\n" +
            "              \"app\": \"bq_backup_manager\",\n" +
            "              \"tracking_id\": \"test-tracking-id\",\n" +
            "              \"table_spec\": \"project.dataset.table\"\n" +
            "            }\n" +
            "          },\n" +
            "          \"jobName\": {\n" +
            "            \"jobId\": \"bq_backup_manager_export_1675960287852-F-4999ca89-d448-47d2-81de-fc5afba8e484\",\n" +
            "            \"location\": \"EU\",\n" +
            "            \"projectId\": \"project\"\n" +
            "          },\n" +
            "          \"jobStatistics\": {\n" +
            "            \"createTime\": \"2023-02-09T16:31:47.674Z\",\n" +
            "            \"endTime\": \"2023-02-09T16:31:48.399Z\",\n" +
            "            \"startTime\": \"2023-02-09T16:31:47.767Z\",\n" +
            "            \"totalSlotMs\": \"228\"\n" +
            "          },\n" +
            "          \"jobStatus\": {\n" +
            "            \"error\": \"{}\",\n" +
            "            \"state\": \"DONE\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"serviceName\": \"bigquery.googleapis.com\",\n" +
            "    \"status\": \"status\"\n" +
            "  },\n" +
            "  \"receiveTimestamp\": \"2023-02-09T16:31:49.213113343Z\",\n" +
            "  \"resource\": {\n" +
            "    \"labels\": {\n" +
            "      \"project_id\": \"test-project\"\n" +
            "    },\n" +
            "    \"type\": \"bigquery_resource\"\n" +
            "  },\n" +
            "  \"severity\": \"INFO\",\n" +
            "  \"timestamp\": \"2023-02-09T16:31:48.416574Z\"\n" +
            "}";

    @Test
    public void testParsingJobCompletionEvent() {

        assertEquals(
                TaggerController.getGcsExportJobLabel(testEvent, "tracking_id"),
                "test-tracking-id"
        );

        assertEquals(
                TaggerController.getGcsExportJobLabel(testEvent, "table_spec"),
                "project.dataset.table"
        );

        assertEquals(
                TaggerController.getGcsExportJobId(testEvent),
                "bq_backup_manager_export_1675960287852-F-4999ca89-d448-47d2-81de-fc5afba8e484"
        );


        assertEquals(
                TaggerController.getGcsExportJobError(testEvent),
                "NA"
        );


    }
}
