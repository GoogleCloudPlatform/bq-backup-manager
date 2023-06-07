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
package com.google.cloud.pso.bq_snapshot_manager.tagger;

import com.google.cloud.Tuple;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.PubSubEvent;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter.BigQuerySnapshoter;
import com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter.GCSSnapshoter;
import com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger.Tagger;
import com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger.TaggerResponse;
import com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger.TaggerRequest;
import com.google.cloud.pso.bq_snapshot_manager.helpers.ControllerExceptionHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.TrackingHelper;
import com.google.cloud.pso.bq_snapshot_manager.services.catalog.DataCatalogServiceImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.map.GcsPersistentMapImpl;
import com.google.cloud.pso.bq_snapshot_manager.services.map.PersistentMap;
import com.google.cloud.pso.bq_snapshot_manager.services.set.GCSPersistentSetImpl;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
public class TaggerController {

    private final LoggingHelper logger;
    private static final Integer functionNumber = 4;
    private Gson gson;
    Environment environment;

    public TaggerController() {

        gson = new Gson();
        environment = new Environment();
        logger = new LoggingHelper(
                TaggerController.class.getSimpleName(),
                functionNumber,
                environment.getProjectId()
        );
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    public ResponseEntity receiveMessage(@RequestBody PubSubEvent requestBody) {

        String trackingId = TrackingHelper.MIN_RUN_ID;
        DataCatalogServiceImpl dataCatalogService = null;

        // These values will be updated based on the execution flow and logged at the end
        ResponseEntity responseEntity;
        TaggerRequest taggerRequest = null;
        TaggerResponse taggerResponse = null;
        boolean isSuccess;
        Exception error = null;
        boolean isRetryableError= false;

        try {

            if (requestBody == null || requestBody.getMessage() == null) {
                String msg = "Bad Request: invalid message format";
                logger.logSevereWithTracker(trackingId, null, msg);
                throw new NonRetryableApplicationException("Request body or message is Null.");
            }

            String requestJsonString = requestBody.getMessage().dataToUtf8String();

            logger.logInfoWithTracker(trackingId, null, String.format("Received payload: %s", requestJsonString));

            // The received pubsub message could have been sent by two different sources
            // 1. BigQuery Snapshoter: as a TaggerRequest JSON payload
            // 2. From a log sink listening for BQ export job completion events. These jobs are originally submitted by the GCS Snapshoter

            boolean isGCSExportJobMessage = isGCSExportJobMessage(requestJsonString);
            if(isGCSExportJobMessage){
                // parse the pubsub request as a BQ Export job completion notification

                String jobId = getGcsExportJobId(requestJsonString);
                String jobProjectId = getGcsExportJobProjectId(requestJsonString);
                trackingId = TrackingHelper.parseTrackingIdFromBQExportJobId(jobId);
                boolean isSuccessfulJob = isSuccessfulJob(requestJsonString);
                String jobError = getGcsExportJobError(requestJsonString);

                PersistentMap persistentMap = new GcsPersistentMapImpl(environment.getGcsFlagsBucket());
                String taggerRequestFile = String.format("%s/%s", "snapshoter-gcs-tagger-requests", jobId);
                String taggerRequestJson = persistentMap.get(taggerRequestFile);
                taggerRequest = gson.fromJson(taggerRequestJson, TaggerRequest.class);

                // After parsing the taggerRequest for tracking, throw a non retryable exception if the backup job failed
                if (!isSuccessfulJob){
                    String msg = String.format("GCS export job '%s' on project '%s' has failed with error `%s`. Please check the BigQuery logs in the backup project where the job ran.",
                            jobId,
                            jobProjectId,
                            jobError
                    );
                    throw new NonRetryableApplicationException(msg);
                }

            }else{
                // parse the pubsub request as a taggerRequest (from BQ Snapshoter)
                taggerRequest = gson.fromJson(requestJsonString, TaggerRequest.class);
            }

            trackingId = taggerRequest.getTrackingId();

            logger.logInfoWithTracker(taggerRequest.isDryRun(), trackingId, taggerRequest.getTargetTable(), String.format("Parsed Request: %s", taggerRequest.toString()));

            dataCatalogService = new DataCatalogServiceImpl();
            Tagger tagger = new Tagger(
                    new LoggingHelper(Tagger.class.getSimpleName(), functionNumber, environment.getProjectId()),
                    environment.toConfig(),
                    dataCatalogService,
                    new GCSPersistentSetImpl(environment.getGcsFlagsBucket()),
                    "tagger-flags"
            );

            taggerResponse = tagger.execute(
                    taggerRequest,
                    requestBody.getMessage().getMessageId()
            );

            responseEntity = new ResponseEntity("Process completed successfully.", HttpStatus.OK);
            isSuccess = true;
        } catch (Exception e) {

            Tuple<ResponseEntity, Boolean> handlingResults  = ControllerExceptionHelper.handleException(e,
                    logger,
                    trackingId,
                    taggerRequest == null? null: taggerRequest.getTargetTable()
                    );
            isSuccess = false;
            responseEntity = handlingResults.x();
            isRetryableError = handlingResults.y();
            error = e;

        }finally {
            if(dataCatalogService != null){
                dataCatalogService.shutdown();
            }
        }

        logger.logUnified(
                taggerRequest == null? null: taggerRequest.isDryRun(),
                functionNumber.toString(),
                taggerRequest == null? null: taggerRequest.getRunId(),
                taggerRequest == null? null: taggerRequest.getTrackingId(),
                taggerRequest == null? null : taggerRequest.getTargetTable(),
                taggerRequest,
                taggerResponse,
                isSuccess,
                error,
                isRetryableError
        );

        return responseEntity;
    }

    public boolean isGCSExportJobMessage(String jsonStr){
        try{
            getGcsExportJobId(jsonStr);
            return true;
        }catch (Exception ex){
            return false;
        }
    }

    public static String getGcsExportJobError(String jsonStr){

        JsonObject errorObject =  JsonParser.parseString(jsonStr)
                .getAsJsonObject().get("protoPayload")
                .getAsJsonObject().get("serviceData")
                .getAsJsonObject().get("jobCompletedEvent")
                .getAsJsonObject().get("job")
                .getAsJsonObject().get("jobStatus")
                .getAsJsonObject().get("error").getAsJsonObject();

        if(errorObject.has("message")){
            return errorObject.get("message").getAsString();
        }else{
            return "";
        }
    }

    public static boolean isSuccessfulJob(String jsonStr){

        // if job has error message then it's not successful
        return !JsonParser.parseString(jsonStr)
                .getAsJsonObject().get("protoPayload")
                .getAsJsonObject().get("serviceData")
                .getAsJsonObject().get("jobCompletedEvent")
                .getAsJsonObject().get("job")
                .getAsJsonObject().get("jobStatus")
                .getAsJsonObject().get("error")
                .getAsJsonObject().has("message");
    }

    public static String getGcsExportJobId(String jsonStr){

        return JsonParser.parseString(jsonStr)
                .getAsJsonObject().get("protoPayload")
                .getAsJsonObject().get("serviceData")
                .getAsJsonObject().get("jobCompletedEvent")
                .getAsJsonObject().get("job")
                .getAsJsonObject().get("jobName")
                .getAsJsonObject().get("jobId").getAsString();
    }

    public static String getGcsExportJobProjectId(String jsonStr){

        return JsonParser.parseString(jsonStr)
                .getAsJsonObject().get("protoPayload")
                .getAsJsonObject().get("serviceData")
                .getAsJsonObject().get("jobCompletedEvent")
                .getAsJsonObject().get("job")
                .getAsJsonObject().get("jobName")
                .getAsJsonObject().get("projectId").getAsString();
    }

    public static String getGcsExportJobLabel(String jsonStr, String label){

        return JsonParser.parseString(jsonStr)
                .getAsJsonObject().get("protoPayload")
                .getAsJsonObject().get("serviceData")
                .getAsJsonObject().get("jobCompletedEvent")
                .getAsJsonObject().get("job")
                .getAsJsonObject().get("jobConfiguration")
                .getAsJsonObject().get("labels").getAsJsonObject()
                .get(label).getAsString();
    }

    public static void main(String[] args) {
        SpringApplication.run(TaggerController.class, args);
    }
}
