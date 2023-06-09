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

package com.google.cloud.pso.bq_snapshot_manager.services.backup_policy;

import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicy;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;

import javax.annotation.Nullable;
import java.io.IOException;

public interface BackupPolicyService {

    void createOrUpdateBackupPolicyForTable(TableSpec tableSpec, BackupPolicy backupPolicy) throws IOException;

    @Nullable BackupPolicy getBackupPolicyForTable(TableSpec tableSpec) throws IOException, IllegalArgumentException;

     void shutdown();
}
