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

package com.google.cloud.pso.bq_snapshot_manager.helpers;

public class ThrowableInfo {

    private Throwable throwable;
    private boolean isRetryable;
    private String notes;

    public ThrowableInfo(Throwable throwable, boolean isRetryable, String notes) {
        this.throwable = throwable;
        this.isRetryable = isRetryable;
        this.notes = notes;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public boolean isRetryable() {
        return isRetryable;
    }

    public String getNotes() {
        return notes;
    }

    @Override
    public String toString() {
        return "ThrowableInfo{" +
                "exception=" + throwable +
                ", isRetryable=" + isRetryable +
                ", notes='" + notes + '\'' +
                '}';
    }
}
