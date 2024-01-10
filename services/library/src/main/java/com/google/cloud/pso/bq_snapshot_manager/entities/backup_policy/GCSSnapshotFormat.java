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

package com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy;


import com.google.cloud.Tuple;
import org.apache.commons.lang3.NotImplementedException;

public enum GCSSnapshotFormat {
    CSV,
    CSV_GZIP,
    JSON,
    JSON_GZIP,
    AVRO,
    AVRO_DEFLATE,
    AVRO_SNAPPY,
    PARQUET,
    PARQUET_SNAPPY,
    PARQUET_GZIP;

    public static Tuple<String, String> getFormatAndCompression(GCSSnapshotFormat format) {

        switch (format) {
            case CSV:
                return Tuple.of("CSV", null);
            case CSV_GZIP:
                return Tuple.of("CSV", "GZIP");
            case JSON:
                return Tuple.of("NEWLINE_DELIMITED_JSON", null);
            case JSON_GZIP:
                return Tuple.of("NEWLINE_DELIMITED_JSON", "GZIP");
            case AVRO:
                return Tuple.of("AVRO", null);
            case AVRO_DEFLATE:
                return Tuple.of("AVRO", "DEFLATE");
            case AVRO_SNAPPY:
                return Tuple.of("AVRO", "SNAPPY");
            case PARQUET:
                return Tuple.of("PARQUET", null);
            case PARQUET_SNAPPY:
                return Tuple.of("PARQUET", "SNAPPY");
            case PARQUET_GZIP:
                return Tuple.of("PARQUET", "GZIP");
            default:
                throw new NotImplementedException(
                        String.format("Format '%s' doesn't have a BQ Export mapping", format));
        }
    }
}
