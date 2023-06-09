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

package com.google.cloud.pso.bq_snapshot_manager.services.scan;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudresourcemanager.v3.model.Project;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.Tuple;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.TableDefinition;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.api.services.cloudresourcemanager.v3.CloudResourceManager;
import com.google.api.services.iam.v1.IamScopes;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicy;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicyFields;
import com.google.cloud.pso.bq_snapshot_manager.functions.f02_configurator.Configurator;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import jdk.jshell.execution.Util;

public class ResourceScannerImpl implements ResourceScanner {

    public static final String DATASTORE_KIND = "project_folder_cache";

    public static final String PROJECT_FOLDER_LKP_SRC_API = "api";

    public static final String PROJECT_FOLDER_LKP_SRC_CACHE = "cache";

    private final BigQuery bqService;
    private final CloudResourceManager cloudResourceManager;

    private Datastore datastore;

    public ResourceScannerImpl() throws IOException, GeneralSecurityException {

        bqService = BigQueryOptions.getDefaultInstance().getService();
        cloudResourceManager = createCloudResourceManagerService();
        datastore = DatastoreOptions.getDefaultInstance().getService();
    }

    @Override
    public List<String> listTables(String projectId, String datasetId) {
        return StreamSupport.stream(bqService.listTables(DatasetId.of(projectId, datasetId)).iterateAll().spliterator(),
                        false)
                .filter(t -> t.getDefinition().getType().equals(TableDefinition.Type.TABLE))
                .map(t -> String.format("%s.%s.%s", projectId, datasetId, t.getTableId().getTable()))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public List<String> listDatasets(String projectId) {
        return StreamSupport.stream(bqService.listDatasets(projectId)
                                .iterateAll()
                                .spliterator(),
                        false)
                .map(d -> String.format("%s.%s", projectId, d.getDatasetId().getDataset()))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public List<String> listProjects(Long folderId) throws IOException {

        List<Project> projects = cloudResourceManager.projects().list()
                .setParent("folders/" + folderId)
                .execute()
                .getProjects();

        return projects
                .stream()
                .map(Project::getProjectId)
                .collect(Collectors.toCollection(ArrayList::new));

    }


    /**
     * Returns the folder id of the direct parent of a project.
     * If the project doesn't have a folder, it returns null.
     * If the project doesn't exist it will throw an exception
     *
     * returns a Tuple of (folder ID, source (cache | api))
     */

    /**
     * Returns the folder id of the direct parent of a project.
     *      * If the project doesn't have a folder, it returns null.
     *      * If the project doesn't exist it will throw an exception
     * @param projectId project id to lookup it's folder id
     * @param runId unique identifier of the run that is used to create keys for the cache
     * @return Tuple of Tuple<String, String> where x = folder id and y = source of the lookup operation (api | cache)
     */
    @Override
    public Tuple<String, String> getParentFolderId(String projectId, String runId) throws IOException {

        /**
         * Resource Manager API has a rate limit of 10 GET operations per second. This means that
         * looking up the folder for each table (for thousands of tables) is not scalable.
         * For that we use a cache layer to store the project-folder pairs in the scope of each run (to address cache invalidation)
         */

        // 1. Lookup the project in the cache

        // construct a key including the pro
        String keyStr = generateProjectFolderCacheKey(projectId, runId);

        Key projectFolderKey = datastore.newKeyFactory().setKind(DATASTORE_KIND).newKey(keyStr);
        Entity projectFolderEntity = datastore.get(projectFolderKey);

        if(projectFolderEntity == null){
            // 2.a project-folder entity doesn't exist in the cache

            // 2.a.1. Query the Resource Manager API
            String parentFolderFromApi = cloudResourceManager
                    .projects()
                    .get(String.format("projects/%s", projectId))
                    .execute()
                    .getParent();

            // API returns "folders/folder_name" and we just return folder_name
            String parentFolderFinal = parentFolderFromApi.startsWith("folders/")?
                    parentFolderFromApi.substring(8):
                    null;

            Timestamp now = Timestamp.now();
            // 2.a.2. Add it to the cache
            projectFolderEntity = Entity.newBuilder(projectFolderKey)
                    .set("project", projectId)
                    .set("parent_folder", parentFolderFinal)
                    .set("run_id", runId)
                    .set("updated_at", now)
                    .set("expires_at", Utils.addSeconds(now, Utils.SECONDS_IN_DAY)) // TTL 1 day
                    .build();

            datastore.put(projectFolderEntity);

            // 2.a.3 return it to the caller
            return Tuple.of(parentFolderFinal, PROJECT_FOLDER_LKP_SRC_API);
        }else{

            String projectFolderFromCache = projectFolderEntity
                    .getValue("parent_folder")
                    .get()
                    .toString();

            // project-folder entity exist in the cache
            // 2.b.1 Return from cache
            return Tuple.of(projectFolderFromCache, PROJECT_FOLDER_LKP_SRC_CACHE);
        }
    }

    public static String generateProjectFolderCacheKey(String project, String runId){
        return String.format("%s_%s", project, runId);
    }

    public static CloudResourceManager createCloudResourceManagerService()
            throws IOException, GeneralSecurityException {
        // Use the Application Default Credentials strategy for authentication. For more info, see:
        // https://cloud.google.com/docs/authentication/production#finding_credentials_automatically
        GoogleCredentials credential =
                GoogleCredentials.getApplicationDefault()
                        .createScoped(Collections.singleton(IamScopes.CLOUD_PLATFORM));

        CloudResourceManager service =
                new CloudResourceManager.Builder(
                        GoogleNetHttpTransport.newTrustedTransport(),
                        JacksonFactory.getDefaultInstance(),
                        new HttpCredentialsAdapter(credential))
                        .setApplicationName("service-accounts")
                        .build();
        return service;
    }
}