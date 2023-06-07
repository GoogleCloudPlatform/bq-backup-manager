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

package com.google.cloud.pso.bq_snapshot_manager.helpers;

import com.google.cloud.Timestamp;
import com.google.cloud.pso.bq_snapshot_manager.entities.Globals;

import java.util.UUID;

public class TrackingHelper {

    // so that it never appears in max(run_id) queries
    public static final String MIN_RUN_ID = "0000000000000-z";

    private static final String heartBeatRunSuffix = "-H";

    private static final String forcedRunSuffix = "-F";

    private static final String dryRunSuffix = "-D";
    private static final Integer suffixLength = 2;

    public static String generateHeartBeatRunId(){
        return generateRunId(heartBeatRunSuffix);
    }

    public static String generateForcedRunId(){
        return generateRunId(forcedRunSuffix);
    }

    public static String generateDryRunId(){
        return generateRunId(dryRunSuffix);
    }

    private static String generateRunId(String suffix){
        return String.format("%s%s", System.currentTimeMillis(), suffix);
    }

    public static String parseRunIdAsPrefix(String runId){
        // currentTimeMillis() will always be 13 chars between Sep 9 2001 at 01:46:40.000 UTC and Nov 20 2286 at 17:46:39.999 UTC
        return runId.substring(0, (13 + suffixLength));
    }

    public static Long parseRunIdAsMilliSeconds(String runId){
        // currentTimeMillis() will always be 13 chars between Sep 9 2001 at 01:46:40.000 UTC and Nov 20 2286 at 17:46:39.999 UTC
        return Long.valueOf(runId.substring(0, 13));
    }

    public static Timestamp parseRunIdAsTimestamp(String runId){
        return Timestamp.ofTimeSecondsAndNanos(
                parseRunIdAsMilliSeconds(runId)/1000,
                0
        );
    }

    public static String generateTrackingId (String runId){
        return String.format("%s-%s", runId, UUID.randomUUID().toString());
    }

    public static String generateBQExportJobId(String trackingId){
        return String.format("%s_%s_%s",trackingId, "export", Globals.APPLICATION_NAME, trackingId);
    }

    public static String parseTrackingIdFromBQExportJobId(String bqExportJobId){
        return Utils.tokenize(bqExportJobId,"_", true).get(0);
    }

    public static String generateBQSnapshotJobId(String trackingId){
        return String.format("%s_%s_%s",trackingId, "snapshot", Globals.APPLICATION_NAME, trackingId);
    }

}
