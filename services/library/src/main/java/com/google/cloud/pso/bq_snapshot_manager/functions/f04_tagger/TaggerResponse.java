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

package com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger;

import com.google.cloud.pso.bq_snapshot_manager.entities.TableOperationRequestResponse;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicyAndState;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupState;

import java.util.Objects;

public class TaggerResponse extends TableOperationRequestResponse {

  private final BackupPolicyAndState originalPolicyAndState;
  private final BackupPolicyAndState updatedPolicyAndState;

  public TaggerResponse(
      TableSpec targetTable,
      String runId,
      String trackingId,
      boolean isDryRun,
      BackupPolicyAndState originalPolicyAndState,
      BackupPolicyAndState updatedPolicyAndState) {
    super(targetTable, runId, trackingId, isDryRun);
    this.originalPolicyAndState = originalPolicyAndState;
    this.updatedPolicyAndState = updatedPolicyAndState;
  }

  public BackupPolicyAndState getOriginalPolicyAndState() {
    return originalPolicyAndState;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    TaggerResponse that = (TaggerResponse) o;
    return originalPolicyAndState.equals(that.originalPolicyAndState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), originalPolicyAndState);
  }

  @Override
  public String toString() {
    return "TaggerResponse{"
        + "originalBackupPolicyAndState="
        + originalPolicyAndState
        + ","
        + "updatedBackupPolicyAndState="
        + updatedPolicyAndState
        + "} "
        + super.toString();
  }
}
