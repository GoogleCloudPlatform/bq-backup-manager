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

package com.google.cloud.pso.bq_snapshot_manager.services.pubsub;


import com.google.cloud.pso.bq_snapshot_manager.entities.JsonMessage;

public class FailedPubSubMessage extends JsonMessage {

    private JsonMessage msg;
    private String exceptionMessage;
    private String exceptionClass;

    public FailedPubSubMessage(JsonMessage msg, Exception exception) {
        this.msg = msg;
        this.exceptionClass = exception != null ? exception.getClass().getName() : null;
        this.exceptionMessage = exception != null ? exception.getMessage() : null;
    }

    public JsonMessage getMsg() {
        return msg;
    }

    public String getExceptionClass() {
        return exceptionClass;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    @Override
    public String toString() {
        return "FailedPubSubMessage{"
                + "msg="
                + msg
                + ", exceptionMessage='"
                + exceptionMessage
                + '\''
                + ", exceptionClass='"
                + exceptionClass
                + '\''
                + '}';
    }
}
