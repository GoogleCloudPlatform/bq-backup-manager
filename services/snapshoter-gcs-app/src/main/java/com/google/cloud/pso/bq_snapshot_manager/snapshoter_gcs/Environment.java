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
package com.google.cloud.pso.bq_snapshot_manager.snapshoter_gcs;

import com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter.SnapshoterConfig;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;

public class Environment {



    public SnapshoterConfig toConfig (){

        return new SnapshoterConfig(
                getProjectId(),
                getOutputTopic()
        );
    }


    public String getProjectId(){
        return Utils.getConfigFromEnv("PROJECT_ID", true);
    }

    public String getOutputTopic() { return Utils.getConfigFromEnv("OUTPUT_TOPIC", true); }

    public String getGcsFlagsBucket(){
        return Utils.getConfigFromEnv("GCS_FLAGS_BUCKET", true);
    }
}
