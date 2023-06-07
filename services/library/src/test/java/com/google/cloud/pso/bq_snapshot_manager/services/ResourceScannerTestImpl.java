/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.pso.bq_snapshot_manager.services;

import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.services.scan.ResourceScanner;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ResourceScannerTestImpl implements ResourceScanner {
    @Override
    public List<String> listProjects(Long folderId) throws NonRetryableApplicationException, GeneralSecurityException, IOException {

        switch (folderId.toString()){
            case "1": return Arrays.asList("p1","p2");
            case "2": return Arrays.asList("p3","p4");
            default: return new ArrayList<>();
        }
    }

    @Override
    public List<String> listDatasets(String project) throws NonRetryableApplicationException, InterruptedException {

        switch (project){
            case "p1": return Arrays.asList("p1.d1","p1.d2");
            case "p2": return Arrays.asList("p2.d1","p2.d2");
            case "p3": return Arrays.asList("p3.d1");
            case "p4": return Arrays.asList("p4.d1");
            default: return new ArrayList<>();
        }
    }

    @Override
    public List<String> listTables(String project, String dataset) throws InterruptedException, NonRetryableApplicationException {

        String projectDataset = String.format("%s.%s", project, dataset);

        switch (projectDataset){
            case "p1.d1": return Arrays.asList("p1.d1.t1","p1.d1.t2");
            case "p1.d2": return Arrays.asList("p1.d2.t1","p1.d2.t2");
            case "p2.d1": return Arrays.asList("p2.d1.t1","p2.d1.t2");
            case "p2.d2": return Arrays.asList("p2.d2.t1","p2.d2.t2");
            case "p3.d1": return Arrays.asList("p3.d1.t1");
            case "p4.d1": return Arrays.asList("p4.d1.t1");
            default: return new ArrayList<>();
        }
    }
}
