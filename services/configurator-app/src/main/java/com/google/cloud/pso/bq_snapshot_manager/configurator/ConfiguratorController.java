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
package com.google.cloud.pso.bq_snapshot_manager.configurator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.Tuple;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.PubSubEvent;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.FallbackBackupPolicy;
import com.google.cloud.pso.bq_snapshot_manager.functions.f02_configurator.Configurator;
import com.google.cloud.pso.bq_snapshot_manager.functions.f02_configurator.ConfiguratorRequest;
import com.google.cloud.pso.bq_snapshot_manager.functions.f02_configurator.ConfiguratorResponse;
import com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger.TaggerRequest;
import com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger.TaggerResponse;
import com.google.cloud.pso.bq_snapshot_manager.helpers.ControllerExceptionHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.TrackingHelper;
import com.google.cloud.pso.bq_snapshot_manager.services.catalog.DataCatalogServiceImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubServiceImpl;
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
public class ConfiguratorController {

    private final LoggingHelper logger;

    private static final Integer functionNumber = 2;

    private Gson gson;
    private Environment environment;
    private FallbackBackupPolicy fallbackBackupPolicy;
    private String trackingId = TrackingHelper.MIN_RUN_ID;

    public ConfiguratorController() throws JsonProcessingException {

        gson = new Gson();
        environment = new Environment();
        logger = new LoggingHelper(
                ConfiguratorController.class.getSimpleName(),
                functionNumber,
                environment.getProjectId()
        );

        logger.logInfoWithTracker(
                trackingId,
                null,
                "Will try to parse fallback backup policy.."
        );

        // initializing in the constructor to fail the Cloud Run deployment
        // if the parsing failed. This is to avoid running the
        // solution silently with invalid cofiguration
        fallbackBackupPolicy = FallbackBackupPolicy.fromJson(environment.getBackupPolicyJson());

        logger.logInfoWithTracker(
                trackingId,
                null,
                "Successfully parsed fallback backup policy"
        );
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    public ResponseEntity receiveMessage(@RequestBody PubSubEvent requestBody) {


        DataCatalogServiceImpl dataCatalogService = null;

        // These values will be updated based on the execution flow and logged at the end
        ResponseEntity responseEntity;
        ConfiguratorRequest configuratorRequest = null;
        ConfiguratorResponse configuratorResponse = null;
        boolean isSuccess;
        Exception error = null;
        boolean isRetryableError= false;

        try {

            if (requestBody == null || requestBody.getMessage() == null) {
                String msg = "Bad Request: invalid message format";
                logger.logSevereWithTracker(trackingId,null, msg);
                throw new NonRetryableApplicationException("Request body or message is Null.");
            }

            String requestJsonString = requestBody.getMessage().dataToUtf8String();

            // remove any escape characters (e.g. from Terraform
            requestJsonString = requestJsonString.replace("\\", "");

            logger.logInfoWithTracker(trackingId, null, String.format("Received payload: %s", requestJsonString));

            configuratorRequest = gson.fromJson(requestJsonString, ConfiguratorRequest.class);

            trackingId = configuratorRequest.getTrackingId();

            logger.logInfoWithTracker(configuratorRequest.isDryRun(), trackingId, configuratorRequest.getTargetTable(), String.format("Parsed Request: %s", configuratorRequest.toString()));

            dataCatalogService = new DataCatalogServiceImpl();

            Configurator configurator = new Configurator(
                    environment.toConfig(),
                    dataCatalogService,
                    new PubSubServiceImpl(),
                    new GCSPersistentSetImpl(environment.getGcsFlagsBucket()),
                    fallbackBackupPolicy,
                    "configurator-flags",
                    functionNumber
            );

            configuratorResponse = configurator.execute(configuratorRequest, requestBody.getMessage().getMessageId());

            responseEntity = new ResponseEntity("Process completed successfully.", HttpStatus.OK);
            isSuccess = true;

        } catch (Exception e) {

            Tuple<ResponseEntity, Boolean> handlingResults  = ControllerExceptionHelper.handleException(
                    e,
                    logger,
                    trackingId,
                    configuratorRequest == null? null : configuratorRequest.getTargetTable()
                    );
            isSuccess = false;
            responseEntity = handlingResults.x();
            isRetryableError = handlingResults.y();
            error = e;

        } finally {

            if (dataCatalogService != null) {
                dataCatalogService.shutdown();
            }
        }

        logger.logUnified(
                configuratorRequest == null? null: configuratorRequest.isDryRun(),
                functionNumber.toString(),
                configuratorRequest == null? null: configuratorRequest.getRunId(),
                configuratorRequest == null? null: configuratorRequest.getTrackingId(),
                configuratorRequest == null? null : configuratorRequest.getTargetTable(),
                configuratorRequest,
                configuratorResponse,
                isSuccess,
                error,
                isRetryableError
        );

        return responseEntity;
    }

    public static void main(String[] args) {
        SpringApplication.run(ConfiguratorController.class, args);
    }
}
