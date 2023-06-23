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

package com.google.cloud.pso.bq_snapshot_manager.services;

import com.google.cloud.pso.bq_snapshot_manager.entities.JsonMessage;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.FailedPubSubMessage;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubPublishResults;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubService;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.SuccessPubSubMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class PubSubServiceTestImpl implements PubSubService {

    @Override
    public PubSubPublishResults publishTableOperationRequests(String projectId, String topicId, List<JsonMessage> messages) throws IOException, InterruptedException {
        List<SuccessPubSubMessage> successPubSubMessages = new ArrayList<>(messages.size());
        for(JsonMessage msg: messages){
            successPubSubMessages.add(
                    new SuccessPubSubMessage(msg, "message-id")
            );
        }
        return new PubSubPublishResults(
                successPubSubMessages,
                new ArrayList<FailedPubSubMessage>()
        );
    }
}
