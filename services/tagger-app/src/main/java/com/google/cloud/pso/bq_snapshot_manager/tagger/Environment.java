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
package com.google.cloud.pso.bq_snapshot_manager.tagger;

import com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger.TaggerConfig;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;

public class Environment {

    public TaggerConfig toConfig (){
        return new TaggerConfig(
                getProjectId(),
                getTagTemplateId(),
                getApplicationName()
        );
    }

    public String getProjectId(){
        return Utils.getConfigFromEnv("PROJECT_ID", true);
    }

    public String getTagTemplateId(){
        return Utils.getConfigFromEnv("TAG_TEMPLATE_ID", true);
    }

    public String getGcsFlagsBucket(){
        return Utils.getConfigFromEnv("GCS_FLAGS_BUCKET", true);
    }
    public String getApplicationName(){
        return Utils.getConfigFromEnv("APPLICATION_NAME", true);
    }

    public String getGcsBackupPoliciesBucket(){
        return Utils.getConfigFromEnv("GCS_BACKUP_POLICIES_BUCKET", true);
    }
}
