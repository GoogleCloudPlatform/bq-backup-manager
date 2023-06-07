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

package com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger;

public class TaggerConfig {

    private String projectId;
    private String tagTemplateId;

    public TaggerConfig(String projectId,
                        String tagTemplateId) {
        this.projectId = projectId;
        this.tagTemplateId = tagTemplateId;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getTagTemplateId() {
        return tagTemplateId;
    }

    @Override
    public String toString() {
        return "TaggerConfig{" +
                "projectId='" + projectId + '\'' +
                ", tagTemplateId='" + tagTemplateId + '\'' +
                '}';
    }
}
