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

package com.google.cloud.pso.bq_snapshot_manager.entities;

import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TableSpecTest {

    @Test
    public void fromFullResource() {

        String input = "//bigquery.googleapis.com/projects/test_project/datasets/test_dataset/tables/test_table";
        TableSpec expected = new TableSpec("test_project", "test_dataset", "test_table");
        TableSpec actual = TableSpec.fromFullResource(input);

        assertEquals(expected, actual);
    }

    @Test
    public void testToDataCatalogLinkedResource(){
        TableSpec table = new TableSpec("test_project", "test_dataset", "test_table");
        String actual = table.toResourceUrl();

        String expected = "https://console.cloud.google.com/bigquery?d=test_dataset&p=test_project&page=table&t=test_table";

        assertEquals(expected, actual);
    }


}
