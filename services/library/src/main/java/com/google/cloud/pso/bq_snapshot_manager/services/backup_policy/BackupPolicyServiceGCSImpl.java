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

import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicy;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicyAndState;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupState;
import com.google.cloud.storage.*;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BackupPolicyServiceGCSImpl implements BackupPolicyService {

    public static final String POLICY_FILE_NAME = "backup_policy.json";
    public static final String STATE_FILE_NAME = "backup_state.json";

    private Storage storage;
    private String bucketName;

    public BackupPolicyServiceGCSImpl(String bucketName) {
        // Instantiates a client
        this.storage = StorageOptions.getDefaultInstance().getService();
        this.bucketName = bucketName;
    }


    public void createOrUpdateBackupPolicyAndStateForTable(TableSpec tableSpec,
                                                           BackupPolicyAndState backupPolicyAndState)
            throws IOException {
        String policyFilePath = tableToBackupPolicyGcsKey(tableSpec);
        String stateFilePath = tableToBackupStateGcsKey(tableSpec);

        writeGCSFileAsUTF8(bucketName, policyFilePath, backupPolicyAndState.getPolicy().toJson());
        writeGCSFileAsUTF8(bucketName, stateFilePath, backupPolicyAndState.getState().toJson());
    }

    private void writeGCSFileAsUTF8(String bucketName, String filePath, String contentStr) throws IOException {
        BlobId blobId = BlobId.of(bucketName, filePath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text").build();

        byte[] contentBytes = contentStr.getBytes(StandardCharsets.UTF_8);
        storage.createFrom(blobInfo, new ByteArrayInputStream(contentBytes));
    }


    public @Nullable BackupPolicyAndState getBackupPolicyAndStateForTable(TableSpec tableSpec) {
        String policyFilePath = tableToBackupPolicyGcsKey(tableSpec);
        String stateFilePath = tableToBackupStateGcsKey(tableSpec);

        // Read policy file (file is expected to be there)
        String policyContent = readGcsFileAsUTF8(bucketName, policyFilePath);
        if(policyContent == null){
            return null;
        }

        // Read state file (file will not be there in the first run)
        String stateContent = readGcsFileAsUTF8(bucketName, stateFilePath);

        return new BackupPolicyAndState(
                BackupPolicy.fromJson(policyContent),
                stateContent == null? null: BackupState.fromJson(stateContent)
        );
    }

    private @Nullable String readGcsFileAsUTF8(String bucketName, String filePath){
        BlobId blobId = BlobId.of(bucketName, filePath);
        Blob blob = storage.get(blobId);
        if (blob == null) {
            return null;
        } else {
            byte[] contentBytes = storage.readAllBytes(blobId);
            return new String(contentBytes, StandardCharsets.UTF_8);
        }
    }

    @Override
    public void shutdown() {
        // do nothing
    }

    public String tableToBackupPolicyGcsKey(TableSpec tableSpec) {
        return String.format("%s/%s/%s" , "policy", tableSpec.toHivePartitionPostfix(), POLICY_FILE_NAME);
    }

    public String tableToBackupStateGcsKey(TableSpec tableSpec) {
        return String.format("%s/%s/%s" , "state", tableSpec.toHivePartitionPostfix(), STATE_FILE_NAME);
    }


}
