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

package com.google.cloud.pso.bq_snapshot_manager.dispatcher;


import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.PubSubEvent;
import com.google.cloud.pso.bq_snapshot_manager.functions.f01_dispatcher.Dispatcher;
import com.google.cloud.pso.bq_snapshot_manager.functions.f01_dispatcher.DispatcherRequest;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.TrackingHelper;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubPublishResults;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubServiceImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.scan.ResourceScannerImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.set.GCSPersistentSetImpl;
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
public class DispatcherController {

    private final LoggingHelper logger;

    private static final Integer functionNumber = 1;

    private Gson gson;
    private Environment environment;

    public DispatcherController() {

        gson = new Gson();
        environment = new Environment();
        logger = new LoggingHelper(
                DispatcherController.class.getSimpleName(),
                functionNumber,
                environment.getProjectId());
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    public ResponseEntity receiveMessage(@RequestBody PubSubEvent requestBody) {

        String runId = TrackingHelper.MIN_RUN_ID;
        String state = "";
        // These values will be updated based on the execution flow and logged at the end

        try {

            if (requestBody == null || requestBody.getMessage() == null) {
                String msg = "Bad Request: invalid message format";
                logger.logSevereWithTracker(runId, null, msg);
                throw new NonRetryableApplicationException("Request body or message is Null.");
            }

            String requestJsonString = requestBody.getMessage().dataToUtf8String();

            // remove any escape characters (e.g. from Terraform
            requestJsonString = requestJsonString.replace("\\", "");

            logger.logInfoWithTracker(runId, null, String.format("Received payload: %s", requestJsonString));

            DispatcherRequest dispatcherRequest = gson.fromJson(requestJsonString, DispatcherRequest.class);

            if(dispatcherRequest.isDryRun()){
                runId = TrackingHelper.generateDryRunId();
            }else{
                if(dispatcherRequest.isForceRun()){
                    runId = TrackingHelper.generateForcedRunId();
                }else{
                    runId = TrackingHelper.generateHeartBeatRunId();
                }
            }

            logger.logInfoWithTracker(dispatcherRequest.isDryRun(), runId, null, String.format("Parsed dispatcher request %s ", dispatcherRequest.toString()));

            Dispatcher dispatcher = new Dispatcher(
                    environment.toConfig(),
                    new PubSubServiceImpl(),
                    new ResourceScannerImpl(),
                    new GCSPersistentSetImpl(environment.getGcsFlagsBucket()),
                    "dispatcher-flags",
                    functionNumber,
                    runId
            );

            PubSubPublishResults results = dispatcher.execute(dispatcherRequest, requestBody.getMessage().getMessageId());

            state = String.format("Publishing results: %s SUCCESS MESSAGES and %s FAILED MESSAGES",
                    results.getSuccessMessages().size(),
                    results.getFailedMessages().size());

            logger.logInfoWithTracker(dispatcherRequest.isDryRun(), runId, null, state);

        } catch (Exception e) {
            logger.logNonRetryableExceptions(runId, null, e);
            state = String.format("ERROR '%s'", e.getMessage());
        }

        // Always ACK the pubsub message to avoid retries
        // The dispatcher is the entry point and retrying it could cause
        // unnecessary runs and costs

        return new ResponseEntity(
                String.format("Process completed with state = %s", state),
                HttpStatus.OK);
    }

    public static void main(String[] args) {
        SpringApplication.run(DispatcherController.class, args);
    }
}

