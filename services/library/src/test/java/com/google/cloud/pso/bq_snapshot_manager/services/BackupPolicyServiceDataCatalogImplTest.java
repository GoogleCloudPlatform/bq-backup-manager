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

package com.google.cloud.pso.bq_snapshot_manager.services;

import com.google.cloud.datacatalog.v1.TagField;
import com.google.cloud.pso.bq_snapshot_manager.services.backup_policy.BackupPolicyServiceDataCatalogImpl;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BackupPolicyServiceDataCatalogImplTest {

    @Test
    public void testConvertTagFieldMapToStrMap() {

        Map<String, TagField> tagMap = new HashMap<>();

        tagMap.put("backup_cron",
                TagField.newBuilder()
                        .setStringValue("test-cron")
                        .build()
        );
        tagMap.put("backup_method",
                TagField.newBuilder()
                        .setEnumValue(TagField.EnumValue.newBuilder()
                                .setDisplayName("BigQuery Snapshot")
                                .build())
                        .build()
        );
        tagMap.put("config_source",
                TagField.newBuilder()
                        .setEnumValue(TagField.EnumValue.newBuilder()
                                .setDisplayName("System")
                                .build())
                        .build()
        );
        tagMap.put("backup_time_travel_offset_days",
                TagField.newBuilder()
                        .setEnumValue(TagField.EnumValue.newBuilder()
                                .setDisplayName("0")
                                .build())
                        .build()
        );
        tagMap.put("backup_project",
                TagField.newBuilder()
                        .setStringValue("test-project")
                        .build()
        );
        tagMap.put("bq_snapshot_storage_dataset",
                TagField.newBuilder()
                        .setStringValue("test-dataset")
                        .build()
        );
        tagMap.put("bq_snapshot_expiration_days",
                TagField.newBuilder()
                        .setDoubleValue(0)
                        .build()
        );
        tagMap.put("gcs_snapshot_storage_location",
                TagField.newBuilder()
                        .setStringValue("test-bucket")
                        .build()
        );

        Map<String, String> expected = new HashMap<>();
        expected.put("backup_cron","test-cron");
        expected.put("backup_method","BigQuery Snapshot");
        expected.put("config_source","System");
        expected.put("backup_time_travel_offset_days","0");
        expected.put("backup_project","test-project");
        expected.put("bq_snapshot_storage_dataset","test-dataset");
        expected.put("bq_snapshot_expiration_days","0.0");
        expected.put("gcs_snapshot_storage_location","test-bucket");

        Map<String, String> actual = BackupPolicyServiceDataCatalogImpl.convertTagFieldMapToStrMap(tagMap);

        assertEquals(expected, actual);
    }


}
