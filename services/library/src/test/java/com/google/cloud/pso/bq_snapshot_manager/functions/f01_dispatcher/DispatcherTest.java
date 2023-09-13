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

package com.google.cloud.pso.bq_snapshot_manager.functions.f01_dispatcher;

import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import com.google.cloud.pso.bq_snapshot_manager.entities.JsonMessage;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.functions.f02_configurator.ConfiguratorRequest;
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
  public void testDispatcher()
      throws IOException, NonRetryableApplicationException, InterruptedException {

    BigQueryScope bigQueryScope =
        new BigQueryScope(
            new ArrayList<>(), // folders - should have no effect
            Arrays.asList("p1", "p2"), // projects include - should have no effect
            new ArrayList<>(), // projects exclude
            Arrays.asList("p1.d2"), // datasets include - should have no effect
            new ArrayList<>(), // datasets exclude
            Arrays.asList("p1.d1.t1", "p1.d1.t2"), // tables include - should be the entry point
            new ArrayList<>() // tables exclude
            );

    List<String> expected = Arrays.asList("p1.d1.t1", "p1.d1.t2");

    // pubsub should publish p1.d1.t1 and p1.d1.t2
    PubSubService pubSubServiceTestImpl =
        new PubSubService() {
          @Override
          public PubSubPublishResults publishTableOperationRequests(
              String projectId, String topicId, List<JsonMessage> messages)
              throws IOException, InterruptedException {

            // check if the pubsubservice received the expected BigQuery Scope from the
            // BigQueryResourceLister in the dispatcher logic
            List<String> tables =
                messages.stream()
                    .map(x -> (((ConfiguratorRequest) x).getTargetTable().toSqlString()))
                    .collect(Collectors.toList());
            assertEquals(expected, tables);

            return new PubSubPublishResults(
                Arrays.asList(
                    new SuccessPubSubMessage(
                        new ConfiguratorRequest(
                            TableSpec.fromSqlString("p1.d1.t1"),
                            "runId",
                            "trackingId",
                            false,
                            false,
                            Timestamp.now()),
                        "publishedMessageId"),
                    new SuccessPubSubMessage(
                        new ConfiguratorRequest(
                            TableSpec.fromSqlString("p1.d1.t2"),
                            "runId",
                            "trackingId",
                            false,
                            false,
                            Timestamp.now()),
                        "publishedMessageId")),
                Arrays.asList(
                    new FailedPubSubMessage(
                        new ConfiguratorRequest(
                            TableSpec.fromSqlString("test.fail.msg"),
                            "runId",
                            "trackingId",
                            false,
                            false,
                            Timestamp.now()),
                        new Exception("test fail message"))));
          }
        };

    ResourceScannerTestImpl resourceScanner = new ResourceScannerTestImpl();
    PersistentSetTestImpl persistentSet = new PersistentSetTestImpl();

    Dispatcher dispatcher =
        new Dispatcher(
            config, pubSubServiceTestImpl, resourceScanner, persistentSet, "dispatcher", 1, runId);

    PubSubPublishResults results =
        dispatcher.execute(new DispatcherRequest(bigQueryScope, false, false), "NA");

    List<String> actual =
        results.getSuccessMessages().stream()
            .map(x -> ((ConfiguratorRequest) x.getMsg()).getTargetTable().toSqlString())
            .collect(Collectors.toList());

    assertEquals(expected, actual);
  }
}
