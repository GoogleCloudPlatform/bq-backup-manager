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

package com.google.cloud.pso.bq_snapshot_manager.services.set;


import com.google.cloud.storage.*;

public class GCSPersistentSetImpl implements PersistentSet {

    private Storage storage;
    private String bucketName;

    public GCSPersistentSetImpl(String bucketName) {
        // Instantiates a client
        this.storage = StorageOptions.getDefaultInstance().getService();
        this.bucketName = bucketName;
    }

    @Override
    public void add(String key) {
        BlobId blobId = BlobId.of(bucketName, key);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo);
    }

    public void remove(String key) {
        BlobId blobId = BlobId.of(bucketName, key);
        storage.delete(blobId);
    }

    @Override
    public boolean contains(String key) {
        BlobId blobId = BlobId.of(bucketName, key);
        Blob blob = storage.get(blobId);
        return blob != null;
    }
}
