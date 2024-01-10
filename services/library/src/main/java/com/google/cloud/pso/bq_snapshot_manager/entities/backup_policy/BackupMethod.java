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

package com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy;


import java.util.Arrays;

public enum BackupMethod {
    BIGQUERY_SNAPSHOT("BigQuery Snapshot"),
    GCS_SNAPSHOT("GCS Snapshot"),
    BOTH("Both");

    private String text;

    BackupMethod(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }

    public static BackupMethod fromString(String text) throws IllegalArgumentException {
        for (BackupMethod b : BackupMethod.values()) {
            if (b.text.equalsIgnoreCase(text)) {
                return b;
            }
        }
        throw new IllegalArgumentException(
                String.format(
                        "Invalid enum text '%s'. Available values are '%s'",
                        text, Arrays.asList(BackupMethod.values())));
    }
}
