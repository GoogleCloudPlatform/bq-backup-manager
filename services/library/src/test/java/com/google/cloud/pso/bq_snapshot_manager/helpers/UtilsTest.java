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
import com.google.cloud.Tuple;
import com.google.cloud.datacatalog.v1.TagField;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupConfigSource;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupMethod;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicy;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.TimeTravelOffsetDays;
import com.google.cloud.pso.bq_snapshot_manager.services.catalog.DataCatalogServiceImpl;
import jdk.jshell.execution.Util;
import org.junit.Test;
import org.springframework.scheduling.support.CronExpression;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class UtilsTest {



    @Test(expected = IllegalArgumentException.class)
    public void getConfigFromEnv_Required() {
        Utils.getConfigFromEnv("NA_VAR", true);
    }

    @Test
    public void getConfigFromEnv_NotRequired() {
        // should not fail because the VAR is not required
        Utils.getConfigFromEnv("NA_VAR", false);
    }

    @Test
    public void testGetTableSpecWithTimeTravel(){
        Timestamp refPoint = Timestamp.parseTimestamp("2022-10-13T12:58:41Z"); // == 1665665921023L

        // test with 0 days (lower bound)
        Tuple<TableSpec, Long> actualWithZero = Utils.getTableSpecWithTimeTravel(
                TableSpec.fromSqlString("p.d.t"),
                TimeTravelOffsetDays.DAYS_0,
                refPoint
        );

        // time travel will be trimmed to seconds
        assertEquals(TableSpec.fromSqlString("p.d.t@1665665921000"), actualWithZero.x());
        assertEquals(1665665921000L, actualWithZero.y().longValue());

        // test with 7 days (upped bound)
        Tuple<TableSpec, Long> actualWith7 = Utils.getTableSpecWithTimeTravel(
                TableSpec.fromSqlString("p.d.t"),
                TimeTravelOffsetDays.DAYS_7,
                refPoint
        );

        Long expectedMs7Days = (1665665921000L - (7 * 86400000)) + 60000;
        assertEquals(TableSpec.fromSqlString("p.d.t@"+expectedMs7Days.toString()), actualWith7.x());
        assertEquals(expectedMs7Days, actualWith7.y());

        // test with 5 days (with bounds)
        Tuple<TableSpec, Long> actualWith5 = Utils.getTableSpecWithTimeTravel(
                TableSpec.fromSqlString("p.d.t"),
                TimeTravelOffsetDays.DAYS_5,
                refPoint
        );

        Long expectedMs5Days = 1665665921000L - (5 * 86400000);
        assertEquals(TableSpec.fromSqlString("p.d.t@"+expectedMs5Days.toString()), actualWith5.x());
        assertEquals(expectedMs5Days, actualWith5.y());

    }

    @Test
    public void testTrimSlashes(){
        assertEquals("bla", Utils.trimSlashes("/bla/"));
        assertEquals("bla", Utils.trimSlashes("bla"));
    }
}