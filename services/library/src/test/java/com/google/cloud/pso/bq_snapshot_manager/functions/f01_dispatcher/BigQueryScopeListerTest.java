/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.pso.bq_snapshot_manager.functions.f01_dispatcher;

import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.services.ResourceScannerTestImpl;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class BigQueryScopeListerTest {

    BigQueryScopeLister lister = new BigQueryScopeLister(
            new ResourceScannerTestImpl(),
            new LoggingHelper("test", 1, "test-project"),
            "R-testxxxxxxx"
    );

    @Test
    public void testOnTableLevel() throws NonRetryableApplicationException {

        BigQueryScope bigQueryScope = new BigQueryScope(
                new ArrayList<>(), // folders - should have no effect
                Arrays.asList("p1", "p2"), // projects include - should have no effect
                new ArrayList<>(), // projects exclude
                Arrays.asList("p1.d2"), // datasets include - should have no effect
                new ArrayList<>(), // datasets exclude
                Arrays.asList("p1.d1.t1", "p1.d1.t2"), // tables include - should be the only scope
                new ArrayList<>() // tables exclude
        );

        List<TableSpec> actual = lister.listTablesInScope(bigQueryScope);
        List<TableSpec> expected = Lists.newArrayList(
                TableSpec.fromSqlString("p1.d1.t1"),
                TableSpec.fromSqlString("p1.d1.t2"));

    }

    @Test
    public void testOnDatasetLevel() throws NonRetryableApplicationException {

        BigQueryScope bigQueryScope = new BigQueryScope(
                new ArrayList<>(), // folders - should have no effect
                Arrays.asList("p1", "p2"), // projects include - should have no effect
                new ArrayList<>(), // projects exclude
                Arrays.asList("p1.d2", "p2.d2"), // datasets include - should be the only include list affecting the scope
                new ArrayList<>(), // datasets exclude
                Arrays.asList("p1.d1.t1", "p1.d1.t2"), // tables include - should have no effect
                Arrays.asList("p1.d2.t1") // tables exclude
        );

        List<TableSpec> actual = lister.listTablesInScope(bigQueryScope);
        // expected all tables under p1.d2 and p2.d2 except for p1.d2.t1
        List<TableSpec> expected = Lists.newArrayList(
                TableSpec.fromSqlString("p1.d2.t2"),
                TableSpec.fromSqlString("p2.d2.t1"),
                TableSpec.fromSqlString("p2.d2.t2")
        );

    }

    @Test
    public void testOnProjectLevel() throws NonRetryableApplicationException {

        BigQueryScope bigQueryScope = new BigQueryScope(
                new ArrayList<>(), // folders - should have no effect
                Arrays.asList("p1", "p2"), // projects include - should be the only include list affecting the scope
                new ArrayList<>(), // projects exclude
                Arrays.asList("p1.d2", "p2.d2"), // datasets include - should have no effect
                Arrays.asList("p1.d1"), // datasets exclude
                Arrays.asList("p1.d1.t1", "p1.d1.t2"), // tables include - should have no effect
                Arrays.asList("p1.d2.t1") // tables exclude
        );

        List<TableSpec> actual = lister.listTablesInScope(bigQueryScope);
        // expected all tables under p1 and p2 except for dataset p1.d1 and table p1.d2.t1
        List<TableSpec> expected = Lists.newArrayList(
                TableSpec.fromSqlString("p1.d2.t2"),

                TableSpec.fromSqlString("p2.d1.t1"),
                TableSpec.fromSqlString("p2.d1.t2"),

                TableSpec.fromSqlString("p2.d2.t1"),
                TableSpec.fromSqlString("p2.d2.t2")
        );

    }

    @Test
    public void testOnFolderLevel() throws NonRetryableApplicationException {

        BigQueryScope bigQueryScope = new BigQueryScope(
                Arrays.asList(1L, 2L), // folders - should be the only include list affecting the scope
                Arrays.asList("p1", "p2"), // projects include - should have no effect
                Arrays.asList("p4"), // projects exclude
                Arrays.asList("p1.d2", "p2.d2"), // datasets include - should have no effect
                Arrays.asList("p1.d1"), // datasets exclude
                Arrays.asList("p1.d1.t1", "p1.d1.t2"), // tables include - should have no effect
                Arrays.asList("p1.d2.t1") // tables exclude
        );

        List<TableSpec> actual = lister.listTablesInScope(bigQueryScope);
        // expected all tables under p1, p2 and p3 except for dataset p1.d1 and table p1.d2.t1
        List<TableSpec> expected = Lists.newArrayList(
                TableSpec.fromSqlString("p1.d2.t2"),

                TableSpec.fromSqlString("p2.d1.t1"),
                TableSpec.fromSqlString("p2.d1.t2"),

                TableSpec.fromSqlString("p2.d2.t1"),
                TableSpec.fromSqlString("p2.d2.t2"),

                TableSpec.fromSqlString("p3.d1.t1")
        );

    }
}
