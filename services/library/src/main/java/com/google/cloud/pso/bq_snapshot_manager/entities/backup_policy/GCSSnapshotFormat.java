package com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy;

import com.google.cloud.Tuple;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupConfigSource;
import org.apache.commons.lang3.NotImplementedException;

import java.util.Arrays;

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

        switch (format){
            case CSV: return Tuple.of("CSV", null);
            case CSV_GZIP: return Tuple.of("CSV", "GZIP");
            case JSON: return Tuple.of("NEWLINE_DELIMITED_JSON", null);
            case JSON_GZIP: return Tuple.of("NEWLINE_DELIMITED_JSON", "GZIP");
            case AVRO: return Tuple.of("AVRO", null);
            case AVRO_DEFLATE: return Tuple.of("AVRO", "DEFLATE");
            case AVRO_SNAPPY: return Tuple.of("AVRO", "SNAPPY");
            case PARQUET: return Tuple.of("PARQUET", null);
            case PARQUET_SNAPPY: return Tuple.of("PARQUET", "SNAPPY");
            case PARQUET_GZIP: return Tuple.of("PARQUET", "GZIP");
            default: throw new NotImplementedException(String.format("Format '%s' doesn't have a BQ Export mapping", format));
        }
    }
}
