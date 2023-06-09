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

import com.google.cloud.Timestamp;
import org.junit.Test;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;

public class TrackingHelperTest {

    @Test
    public void parseAsTimestamp(){
        LocalDateTime now = LocalDateTime.ofEpochSecond(1641034800, 0, ZoneOffset.UTC);
        Timestamp fromRunId = TrackingHelper.parseRunIdAsTimestamp("1641034800000-H");

        assertEquals(1641034800, fromRunId.getSeconds());
    }

    @Test
    public void parseTrackingIdFromJobId(){
        String trackingId = TrackingHelper.generateTrackingId(TrackingHelper.MIN_RUN_ID);
        String parsedTrackingId = TrackingHelper.parseTrackingIdFromBQExportJobId(
                TrackingHelper.generateBQExportJobId(trackingId, "bq_backup_manager")
        );

        assertEquals(trackingId, parsedTrackingId);

    }
}
