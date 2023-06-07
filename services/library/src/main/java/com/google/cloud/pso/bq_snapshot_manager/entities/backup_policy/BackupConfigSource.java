package com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy;

import java.util.Arrays;

public enum BackupConfigSource {
    SYSTEM("SYSTEM"),
    MANUAL("MANUAL");

    private String text;

    BackupConfigSource(String text) {
        this.text = text;
    }

    public String getText() {
        return this.text;
    }

    public static BackupConfigSource fromString(String text) throws IllegalArgumentException {
        for (BackupConfigSource b : BackupConfigSource.values()) {
            if (b.text.equalsIgnoreCase(text)) {
                return b;
            }
        }
        throw new IllegalArgumentException(
                String.format("Invalid enum text '%s'. Available values are '%s'",
                        text,
                        Arrays.asList(BackupConfigSource.values())
                )
        );
    }
}
