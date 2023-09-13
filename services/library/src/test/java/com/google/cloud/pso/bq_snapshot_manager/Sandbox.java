package com.google.cloud.pso.bq_snapshot_manager;

import com.google.cloud.Timestamp;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.*;
import com.google.cloud.pso.bq_snapshot_manager.functions.f02_configurator.ConfiguratorResponse;
import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import org.junit.Test;

public class Sandbox {

    @Test
    public void test(){

        BackupPolicy p = new BackupPolicy.BackupPolicyBuilder("* * * * *", BackupMethod.BIGQUERY_SNAPSHOT,
                TimeTravelOffsetDays.DAYS_0,  BackupConfigSource.SYSTEM, "backupproject")
                .setBigQuerySnapshotStorageDataset("bq")
                .setBigQuerySnapshotExpirationDays(3.0)
                .build();

        BackupState s = new BackupState(Timestamp.now(), "bq://", "gs://");

        ConfiguratorResponse r = new ConfiguratorResponse(
                null, // TableSpec.fromSqlString("a.b.c"),
                "run_id",
                "tracking_id",
                true,
                null, //new BackupPolicyAndState(p,s),
                "backupPolicySource",
                null, //Timestamp.now(),
                true,
                false,
                true,
                null,
                null
        );

        Moshi moshi = new Moshi.Builder().build();
        JsonAdapter<ConfiguratorResponse> jsonAdapter = moshi.adapter(ConfiguratorResponse.class);

        String json = jsonAdapter.toJson(r);
        System.out.println(json);

        //System.out.println(r.toJsonString());
    }
}
