package com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy;



import java.util.Arrays;

public enum BackupMethod {
    BIGQUERY_SNAPSHOT("BigQuery Snapshot"),
    GCS_SNAPSHOT("GCS Snapshot"),
    BOTH("Both");

    private String text;

    BackupMethod(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }

    public static BackupMethod fromString(String text) throws IllegalArgumentException {
        for (BackupMethod b : BackupMethod.values()) {
            if (b.text.equalsIgnoreCase(text)) {
                return b;
            }
        }
        throw new IllegalArgumentException(
                String.format("Invalid enum text '%s'. Available values are '%s'",
                        text,
                        Arrays.asList(BackupMethod.values())
                        )
        );
    }
}
