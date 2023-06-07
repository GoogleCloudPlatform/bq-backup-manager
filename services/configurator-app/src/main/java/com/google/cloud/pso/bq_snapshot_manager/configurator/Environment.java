/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.pso.bq_snapshot_manager.configurator;

import com.google.cloud.pso.bq_snapshot_manager.functions.f02_configurator.ConfiguratorConfig;
import com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter.SnapshoterConfig;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;

public class Environment {



    public ConfiguratorConfig toConfig (){

        return new ConfiguratorConfig(
                getProjectId(),
                getBqSnapshoterOutputTopic(),
                getGCSSnapshoterOutputTopic(),
                getBackupTagTemplateId()
        );
    }

    public String getProjectId(){
        return Utils.getConfigFromEnv("PROJECT_ID", true);
    }

    public String getComputeRegionId(){
        return Utils.getConfigFromEnv("COMPUTE_REGION_ID", true);
    }

    public String getDataRegionId(){
        return Utils.getConfigFromEnv("DATA_REGION_ID", true);
    }

    public String getBackupPolicyJson(){
        return Utils.getConfigFromEnv("BACKUP_POLICY_JSON", true);
    }

    public String getBqSnapshoterOutputTopic() { return Utils.getConfigFromEnv("SNAPSHOTER_BQ_OUTPUT_TOPIC", true); }

    public String getGCSSnapshoterOutputTopic() { return Utils.getConfigFromEnv("SNAPSHOTER_GCS_OUTPUT_TOPIC", true); }

    public String getBackupTagTemplateId() { return Utils.getConfigFromEnv("BACKUP_TAG_TEMPLATE_ID", true); }

    public String getGcsFlagsBucket(){
        return Utils.getConfigFromEnv("GCS_FLAGS_BUCKET", true);
    }
}
