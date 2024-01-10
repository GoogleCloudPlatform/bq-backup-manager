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

package com.google.cloud.pso.bq_snapshot_manager.dispatcher_tables;

import com.google.cloud.Tuple;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.PubSubEvent;
import com.google.cloud.pso.bq_snapshot_manager.functions.f01_2_dispatcher_tables.DispatcherTableRequest;
import com.google.cloud.pso.bq_snapshot_manager.functions.f01_2_dispatcher_tables.DispatcherTables;
import com.google.cloud.pso.bq_snapshot_manager.helpers.ControllerExceptionHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.TrackingHelper;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubPublishResults;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubServiceImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.scan.ResourceScannerImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.set.GCSPersistentSetImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.tracking.GCSObjectTracker;
import com.google.gson.Gson;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication(scanBasePackages = "com.google.cloud.pso.bq_snapshot_manager")
@RestController
public class DispatcherTablesController {

  private final LoggingHelper logger;

  private static final Integer functionNumber = 12;

  private Gson gson;
  private Environment environment;

  public DispatcherTablesController() {

    gson = new Gson();
    environment = new Environment();
    logger =
        new LoggingHelper(
            DispatcherTablesController.class.getSimpleName(),
            functionNumber,
            environment.getProjectId(),
            environment.getApplicationName());
  }

  @RequestMapping(value = "/", method = RequestMethod.POST)
  public ResponseEntity receiveMessage(@RequestBody PubSubEvent requestBody) {

    String runId = TrackingHelper.MIN_RUN_ID;
    String state = "";

    // These values will be updated based on the execution flow and logged at the end
    // These values will be updated based on the execution flow and logged at the end
    ResponseEntity responseEntity;
    DispatcherTableRequest dispatcherRequest = null;
    PubSubPublishResults dispatcherResults = null;
    boolean isSuccess;
    Exception error = null;
    boolean isRetryableError = false;

    try {

      if (requestBody == null || requestBody.getMessage() == null) {
        String msg = "Bad Request: invalid message format";
        logger.logSevereWithTracker(runId, null, msg);
        throw new NonRetryableApplicationException("Request body or message is Null.");
      }

      String requestJsonString = requestBody.getMessage().dataToUtf8String();

      // remove any escape characters (e.g. from Terraform
      requestJsonString = requestJsonString.replace("\\", "");

      logger.logInfoWithTracker(
          runId, null, String.format("Received payload: %s", requestJsonString));

      dispatcherRequest = gson.fromJson(requestJsonString, DispatcherTableRequest.class);

      runId = dispatcherRequest.getRunId();

      logger.logInfoWithTracker(
          dispatcherRequest.isDryRun(),
          runId,
          null,
          String.format("Parsed dispatcher request %s ", dispatcherRequest.toString()));

      DispatcherTables dispatcher =
          new DispatcherTables(
              environment.toConfig(),
              new PubSubServiceImpl(),
              new ResourceScannerImpl(),
              new GCSPersistentSetImpl(environment.getGcsFlagsBucket()),
              new GCSObjectTracker(environment.getDispatchedTablesBucketName()),
              "dispatcher-tables-flags",
              functionNumber,
              runId);

      dispatcherResults =
          dispatcher.execute(dispatcherRequest, requestBody.getMessage().getMessageId());

      state =
          String.format(
              "Publishing results: %s SUCCESS MESSAGES and %s FAILED MESSAGES",
              dispatcherResults.getSuccessMessages().size(),
              dispatcherResults.getFailedMessages().size());

      logger.logInfoWithTracker(dispatcherRequest.isDryRun(), runId, null, state);

      responseEntity = new ResponseEntity("Process completed successfully.", HttpStatus.OK);
      isSuccess = true;

    } catch (Exception e) {

      Tuple<ResponseEntity, Boolean> handlingResults =
          ControllerExceptionHelper.handleException(e, logger, runId, null);
      isSuccess = false;
      responseEntity = handlingResults.x();
      isRetryableError = handlingResults.y();
      error = e;
    }

    logger.logUnified(
        dispatcherRequest == null ? null : dispatcherRequest.isDryRun(),
        functionNumber.toString(),
        dispatcherRequest == null ? null : dispatcherRequest.getRunId(),
        dispatcherRequest == null ? null : dispatcherRequest.getRunId(),
        null,
        dispatcherRequest,
        dispatcherResults,
        isSuccess,
        error,
        isRetryableError);

    return responseEntity;
  }

  public static void main(String[] args) {
    SpringApplication.run(DispatcherTablesController.class, args);
  }
}
