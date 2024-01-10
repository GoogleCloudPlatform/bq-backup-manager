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


import com.google.cloud.pso.bq_snapshot_manager.services.set.PersistentSet;
import java.util.HashSet;
import java.util.Set;

public class PersistentSetTestImpl implements PersistentSet {

    Set<String> set;

    public PersistentSetTestImpl() {
        set = new HashSet<>();
    }

    @Override
    public void add(String key) {
        set.add(key);
    }

    @Override
    public void remove(String key) {
        set.remove(key);
    }

    @Override
    public boolean contains(String key) {
        return set.contains(key);
    }
}
