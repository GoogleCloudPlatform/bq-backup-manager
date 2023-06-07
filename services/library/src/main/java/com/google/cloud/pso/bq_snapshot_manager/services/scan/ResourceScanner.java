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

import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

public interface ResourceScanner {

    // list projects under a folder in the format "project"
    List<String> listProjects(Long folderId) throws NonRetryableApplicationException, GeneralSecurityException, IOException;


    // list datasets under a project in the format "project.dataset"
    List<String> listDatasets(String project) throws NonRetryableApplicationException, InterruptedException;

    // list tables under a project/dataset in the format "project.dataset.table"
    List<String> listTables(String project, String dataset) throws InterruptedException, NonRetryableApplicationException;
}