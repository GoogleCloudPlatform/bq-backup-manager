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

import com.google.cloud.pso.bq_snapshot_manager.entities.JsonMessage;

public class DispatcherRequest extends JsonMessage {

  // take table backup in this run regardless of the cron check
  private boolean isForceRun;

  // no backup or tagging operations, only logging
  private boolean isDryRun;
  private BigQueryScope bigQueryScope;

  public DispatcherRequest(BigQueryScope bigQueryScope, boolean isForceRun, boolean isDryRun) {
    this.isForceRun = isForceRun;
    this.isDryRun = isDryRun;
    this.bigQueryScope = bigQueryScope;
  }

  public boolean isForceRun() {
    return isForceRun;
  }

  public boolean isDryRun() {
    return isDryRun;
  }

  public BigQueryScope getBigQueryScope() {
    return bigQueryScope;
  }

  @Override
  public String toString() {
    return "DispatcherRequest{"
        + "isForceRun="
        + isForceRun
        + "isDryRun="
        + isDryRun
        + ", bigQueryScope="
        + bigQueryScope
        + '}';
  }
}
