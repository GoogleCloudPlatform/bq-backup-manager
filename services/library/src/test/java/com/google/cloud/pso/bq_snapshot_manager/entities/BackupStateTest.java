package com.google.cloud.pso.bq_snapshot_manager.entities;

import com.google.cloud.Timestamp;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BackupStateTest {

    @Test
    public void toJsonTest(){

        BackupState actual = new BackupState(
                Timestamp.MIN_VALUE,
                "last_bq",
                "last_gcs"
        );

        String expected = "{\"last_backup_at\":\"0001-01-01T00:00:00Z\",\"last_bq_snapshot_storage_uri\":\"last_bq\",\"last_gcs_snapshot_storage_uri\":\"last_gcs\"}";

        assertEquals(expected, actual.toJson());
    }

    @Test
    public void fromJsonTest(){

        String actual = "{\"last_backup_at\":\"0001-01-01T00:00:00Z\",\"last_bq_snapshot_storage_uri\":\"last_bq\",\"last_gcs_snapshot_storage_uri\":\"last_gcs\"}";

        BackupState expected = new BackupState(
                Timestamp.MIN_VALUE,
                "last_bq",
                "last_gcs"
        );


        assertEquals(expected, BackupState.fromJson(actual));
    }

    @Test
    public void fromJson_WithNull_Test(){

        String actual = "{\"last_backup_at\":\"0001-01-01T00:00:00Z\",\"last_gcs_snapshot_storage_uri\":\"last_gcs\"}";

        BackupState expected = new BackupState(
                Timestamp.MIN_VALUE,
                null,
                "last_gcs"
        );

        assertEquals(expected, BackupState.fromJson(actual));
    }
}
