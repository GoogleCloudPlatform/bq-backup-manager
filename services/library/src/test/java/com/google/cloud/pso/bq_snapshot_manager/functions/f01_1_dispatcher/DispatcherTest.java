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

package com.google.cloud.pso.bq_snapshot_manager.functions.f01_1_dispatcher;

import static org.junit.Assert.assertEquals;

import com.google.cloud.pso.bq_snapshot_manager.entities.DatasetSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.JsonMessage;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.functions.f01_2_dispatcher_tables.DispatcherTableRequest;
import com.google.cloud.pso.bq_snapshot_manager.services.PersistentSetTestImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.ResourceScannerTestImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class DispatcherTest {

    DispatcherConfig config =
            new DispatcherConfig(
                    "testProjectId",
                    "testComputeRegionId",
                    "testDataRegionId",
                    "testTaggerTopic",
                    "bq_backup_manager");

    String runId = "1679574252412-R";

    @Test
    public void testDispatcherWithTableIncludeList()
            throws IOException, NonRetryableApplicationException, InterruptedException {

        BigQueryScope bigQueryScope =
                new BigQueryScope(
                        new ArrayList<>(), // folders - should have no effect
                        Arrays.asList("p1", "p2"), // projects include - should have no effect
                        new ArrayList<>(), // projects exclude
                        Arrays.asList("p1.d2"), // datasets include - should have no effect
                        new ArrayList<>(), // datasets exclude
                        Arrays.asList(
                                "p1.d1.t1",
                                "p1.d1.t2"), // tables include - should be the entry point
                        new ArrayList<>() // tables exclude
                        );

        List<String> expectedDatasetIncludeList = Arrays.asList("p1.d1.t1", "p1.d1.t2");

        // pubsub should publish a DispatcherTableRequest with tablesIncludeList only and no
        // DatasetSpec
        PubSubService pubSubServiceTestImpl =
                new PubSubService() {
                    @Override
                    public PubSubPublishResults publishTableOperationRequests(
                            String projectId, String topicId, List<JsonMessage> messages)
                            throws IOException, InterruptedException {

                        return new PubSubPublishResults(
                                Arrays.asList(
                                        new SuccessPubSubMessage(
                                                new DispatcherTableRequest(
                                                        "test-run-id",
                                                        false,
                                                        false,
                                                        null,
                                                        Arrays.asList("p1.d1.t1", "p1.d1.t2"),
                                                        new ArrayList<>()),
                                                "publishedMessageId")),
                                new ArrayList<>() // failed pubsub messages
                                );
                    }
                };

        ResourceScannerTestImpl resourceScanner = new ResourceScannerTestImpl();
        PersistentSetTestImpl persistentSet = new PersistentSetTestImpl();

        Dispatcher dispatcher =
                new Dispatcher(
                        config,
                        pubSubServiceTestImpl,
                        resourceScanner,
                        persistentSet,
                        "dispatcher",
                        1,
                        runId);

        PubSubPublishResults results =
                dispatcher.execute(new DispatcherRequest(bigQueryScope, false, false), "NA");

        // there should be only one published message to Dispatcher Table that contains the
        // tableIncludeList
        assertEquals(results.getSuccessMessages().size(), 1);
        DispatcherTableRequest dispatcherTableRequest =
                ((DispatcherTableRequest) results.getSuccessMessages().get(0).getMsg());

        assertEquals(expectedDatasetIncludeList, dispatcherTableRequest.getTablesInclusionList());
    }

    @Test
    public void testDispatcherWithDatasetIncludeList()
            throws IOException, NonRetryableApplicationException, InterruptedException {

        BigQueryScope bigQueryScope =
                new BigQueryScope(
                        new ArrayList<>(), // folders - should have no effect
                        Arrays.asList(
                                "p1", "p2"), // projects include - should be the entry point effect
                        new ArrayList<>(), // projects exclude
                        new ArrayList<>(), // datasets include
                        List.of("p1.d1"), // datasets exclude
                        new ArrayList<>(), // tables include
                        new ArrayList<>() // tables exclude
                        );

        List<String> expectedDatasetIncludeList = Arrays.asList("p1.d2", "p2.d1", "p2.d2");

        // pubsub should publish a DispatcherTableRequest with tablesIncludeList only and no
        // DatasetSpec
        PubSubService pubSubServiceTestImpl =
                new PubSubService() {
                    @Override
                    public PubSubPublishResults publishTableOperationRequests(
                            String projectId, String topicId, List<JsonMessage> messages)
                            throws IOException, InterruptedException {

                        return new PubSubPublishResults(
                                Arrays.asList(
                                        new SuccessPubSubMessage(
                                                new DispatcherTableRequest(
                                                        "test-run-id",
                                                        false,
                                                        false,
                                                        new DatasetSpec("p1", "d2"),
                                                        new ArrayList<>(),
                                                        new ArrayList<>()),
                                                "publishedMessageId"),
                                        new SuccessPubSubMessage(
                                                new DispatcherTableRequest(
                                                        "test-run-id",
                                                        false,
                                                        false,
                                                        new DatasetSpec("p2", "d1"),
                                                        new ArrayList<>(),
                                                        new ArrayList<>()),
                                                "publishedMessageId"),
                                        new SuccessPubSubMessage(
                                                new DispatcherTableRequest(
                                                        "test-run-id",
                                                        false,
                                                        false,
                                                        new DatasetSpec("p2", "d2"),
                                                        new ArrayList<>(),
                                                        new ArrayList<>()),
                                                "publishedMessageId")),
                                new ArrayList<>() // failed pubsub messages
                                );
                    }
                };

        ResourceScannerTestImpl resourceScanner = new ResourceScannerTestImpl();
        PersistentSetTestImpl persistentSet = new PersistentSetTestImpl();

        Dispatcher dispatcher =
                new Dispatcher(
                        config,
                        pubSubServiceTestImpl,
                        resourceScanner,
                        persistentSet,
                        "dispatcher",
                        1,
                        runId);

        PubSubPublishResults results =
                dispatcher.execute(new DispatcherRequest(bigQueryScope, false, false), "NA");

        // there should be only one published message to Dispatcher Table that contains the
        // tableIncludeList
        assertEquals(results.getSuccessMessages().size(), expectedDatasetIncludeList.size());
        List<String> actual =
                results.getSuccessMessages().stream()
                        .map(
                                x ->
                                        ((DispatcherTableRequest) x.getMsg())
                                                .getDatasetSpec()
                                                .toSqlString())
                        .collect(Collectors.toCollection(ArrayList::new));

        assertEquals(expectedDatasetIncludeList, actual);
    }
}
