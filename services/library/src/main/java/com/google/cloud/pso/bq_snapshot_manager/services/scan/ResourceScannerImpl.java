package com.google.cloud.pso.bq_snapshot_manager.services.scan;

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

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudresourcemanager.v3.model.Project;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.TableDefinition;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import com.google.api.services.cloudresourcemanager.v3.CloudResourceManager;
import com.google.api.services.iam.v1.IamScopes;

public class ResourceScannerImpl implements ResourceScanner {

    private BigQuery bqService;
    private CloudResourceManager cloudResourceManager;

    public ResourceScannerImpl() throws IOException, GeneralSecurityException {

        bqService = BigQueryOptions.getDefaultInstance().getService();
        cloudResourceManager = createCloudResourceManagerService();
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
                .setParent("folders/"+folderId)
                .execute()
                .getProjects();

        return projects
                .stream()
                .map(Project::getDisplayName)
                .collect(Collectors.toCollection(ArrayList::new));

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