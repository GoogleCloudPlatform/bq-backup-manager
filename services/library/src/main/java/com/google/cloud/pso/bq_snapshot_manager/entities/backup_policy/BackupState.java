package com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy;


import com.google.cloud.Timestamp;
import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import java.util.HashMap;
import java.util.Map;

public class BackupState {

    @SerializedName("last_backup_at")
    private Timestamp lastBackupAt;

    @SerializedName("last_gcs_snapshot_storage_uri")
    private String lastBqSnapshotStorageUri;

    @SerializedName("last_bq_snapshot_storage_uri")
    private String lastGcsSnapshotStorageUri;

    public BackupState(
            Timestamp lastBackupAt,
            String lastBqSnapshotStorageUri,
            String lastGcsSnapshotStorageUri) {
        this.lastBackupAt = lastBackupAt;
        this.lastBqSnapshotStorageUri = lastBqSnapshotStorageUri;
        this.lastGcsSnapshotStorageUri = lastGcsSnapshotStorageUri;
    }

    public static BackupState fromJson(String jsonStr) {
        // Parse JSON as map and build the fields while applying parsing and conditions
        Gson gson = new Gson();
        Map<String, String> fieldsMap = gson.fromJson(jsonStr, HashMap.class);
        return new BackupState(
                Timestamp.parseTimestamp(fieldsMap.get("last_backup_at")),
                fieldsMap.getOrDefault("last_bq_snapshot_storage_uri", null),
                fieldsMap.getOrDefault("last_gcs_snapshot_storage_uri", null));
    }

    public String toJson() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("last_backup_at", lastBackupAt.toString());
        jsonObject.addProperty("last_bq_snapshot_storage_uri", lastBqSnapshotStorageUri);
        jsonObject.addProperty("last_gcs_snapshot_storage_uri", lastGcsSnapshotStorageUri);

        return jsonObject.toString();
    }

    public Timestamp getLastBackupAt() {
        return lastBackupAt;
    }

    public String getLastBqSnapshotStorageUri() {
        return lastBqSnapshotStorageUri;
    }

    public String getLastGcsSnapshotStorageUri() {
        return lastGcsSnapshotStorageUri;
    }

    public void setLastBackupAt(Timestamp lastBackupAt) {
        this.lastBackupAt = lastBackupAt;
    }

    public void setLastBqSnapshotStorageUri(String lastBqSnapshotStorageUri) {
        this.lastBqSnapshotStorageUri = lastBqSnapshotStorageUri;
    }

    public void setLastGcsSnapshotStorageUri(String lastGcsSnapshotStorageUri) {
        this.lastGcsSnapshotStorageUri = lastGcsSnapshotStorageUri;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BackupState that = (BackupState) o;
        return Objects.equal(lastBackupAt, that.lastBackupAt)
                && Objects.equal(lastBqSnapshotStorageUri, that.lastBqSnapshotStorageUri)
                && Objects.equal(lastGcsSnapshotStorageUri, that.lastGcsSnapshotStorageUri);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(lastBackupAt, lastBqSnapshotStorageUri, lastGcsSnapshotStorageUri);
    }

    @Override
    public String toString() {
        return "BackupState{"
                + "lastBackupAt="
                + lastBackupAt
                + ", lastBqSnapshotStorageUri='"
                + lastBqSnapshotStorageUri
                + '\''
                + ", lastGcsSnapshotStorageUri='"
                + lastGcsSnapshotStorageUri
                + '\''
                + '}';
    }
}
