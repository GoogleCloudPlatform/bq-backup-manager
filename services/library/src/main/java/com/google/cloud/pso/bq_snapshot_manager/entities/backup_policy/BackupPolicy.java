package com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy;

import com.google.api.client.util.Data;
import com.google.cloud.Timestamp;
import com.google.cloud.datacatalog.v1.Tag;
import com.google.cloud.datacatalog.v1.TagField;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import com.google.common.base.Objects;
import com.google.gson.Gson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BackupPolicy {

    private final String cron;
    private final BackupMethod method;
    private final TimeTravelOffsetDays timeTravelOffsetDays;

    private final Double bigQuerySnapshotExpirationDays;
    private final String backupProject;
    private final String bigQuerySnapshotStorageDataset;
    private final String gcsSnapshotStorageLocation;
    private final GCSSnapshotFormat gcsExportFormat;

    private final String gcsCsvDelimiter;

    private final Boolean gcsCsvExportHeader;

    private final Boolean gcsUseAvroLogicalTypes;

    private final BackupConfigSource configSource;
    private Timestamp lastBackupAt;
    private String lastBqSnapshotStorageUri;
    private String lastGcsSnapshotStorageUri;

    public BackupPolicy(BackupPolicyBuilder builder) throws IllegalArgumentException {

        List<DataCatalogBackupPolicyTagFields> missingFields = validate(builder);

        if (!missingFields.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Backup policy is invalid for backup method '%s'. The following fields are missing %s",
                            builder.method,
                            missingFields));
        }

        this.cron = builder.cron;
        this.method = builder.method;
        this.timeTravelOffsetDays = builder.timeTravelOffsetDays;
        this.bigQuerySnapshotExpirationDays = builder.bigQuerySnapshotExpirationDays;
        this.backupProject = builder.backupProject;
        this.bigQuerySnapshotStorageDataset = builder.bigQuerySnapshotStorageDataset;
        this.gcsSnapshotStorageLocation = builder.gcsSnapshotStorageLocation;
        this.gcsExportFormat = builder.gcsExportFormat;
        this.gcsCsvDelimiter = builder.gcsCsvDelimiter;
        this.gcsCsvExportHeader = builder.gcsCsvExportHeader;
        this.gcsUseAvroLogicalTypes = builder.gcsUseAvroLogicalTypes;
        this.configSource = builder.configSource;
        this.lastBackupAt = builder.lastBackupAt;
        this.lastBqSnapshotStorageUri = builder.lastBqSnapshotStorageUri;
        this.lastGcsSnapshotStorageUri = builder.lastGcsSnapshotStorageUri;
    }

    public static List<DataCatalogBackupPolicyTagFields> validate(BackupPolicyBuilder builder) {
        // validate that all required fields are provided depending on the backup method
        List<DataCatalogBackupPolicyTagFields> missingRequired = new ArrayList<>();
        List<DataCatalogBackupPolicyTagFields> missingOptional = new ArrayList<>();

        if (builder.cron == null) {
            missingRequired.add(DataCatalogBackupPolicyTagFields.backup_cron);
        }
        if (builder.method == null) {
            missingRequired.add(DataCatalogBackupPolicyTagFields.backup_method);
        }
        if (builder.timeTravelOffsetDays == null) {
            missingRequired.add(DataCatalogBackupPolicyTagFields.backup_time_travel_offset_days);
        }
        if (builder.configSource == null) {
            missingRequired.add(DataCatalogBackupPolicyTagFields.config_source);
        }
        if (builder.backupProject == null) {
            missingRequired.add(DataCatalogBackupPolicyTagFields.backup_project);
        }

        // if required params are missing return and don't continue with other checks
        if (!missingRequired.isEmpty()) {
            return missingRequired;
        }

        if (builder.method.equals(BackupMethod.BIGQUERY_SNAPSHOT) || builder.method.equals(BackupMethod.BOTH)) {
            if (builder.bigQuerySnapshotStorageDataset == null) {
                missingOptional.add(DataCatalogBackupPolicyTagFields.bq_snapshot_storage_dataset);
            }
            if (builder.bigQuerySnapshotExpirationDays == null) {
                missingOptional.add(DataCatalogBackupPolicyTagFields.bq_snapshot_expiration_days);
            }
        }

        // check for GCS params
        if (builder.method.equals(BackupMethod.GCS_SNAPSHOT) || builder.method.equals(BackupMethod.BOTH)) {

            String format = builder.gcsExportFormat == null ? null
                    : GCSSnapshotFormat.getFormatAndCompression(builder.gcsExportFormat).x();

            if (builder.gcsExportFormat == null) {
                missingOptional.add(DataCatalogBackupPolicyTagFields.gcs_snapshot_format);
            }
            if (builder.gcsSnapshotStorageLocation == null) {
                missingOptional.add(DataCatalogBackupPolicyTagFields.gcs_snapshot_storage_location);
            }
            // check required fields for CSV exports
            if (format != null && format.equals("CSV")) {
                if (builder.gcsCsvDelimiter == null) {
                    missingOptional.add(DataCatalogBackupPolicyTagFields.gcs_csv_delimiter);
                }
                if (builder.gcsCsvExportHeader == null) {
                    missingOptional.add(DataCatalogBackupPolicyTagFields.gcs_csv_export_header);
                }
            }
            // check for required fields for avro export
            if (format != null && format.equals("AVRO")) {
                if (builder.gcsUseAvroLogicalTypes == null) {
                    missingOptional.add(DataCatalogBackupPolicyTagFields.gcs_avro_use_logical_types);
                }
            }
        }

        return missingOptional;
    }

    public String getCron() {
        return cron;
    }

    public BackupMethod getMethod() {
        return method;
    }

    public TimeTravelOffsetDays getTimeTravelOffsetDays() {
        return timeTravelOffsetDays;
    }

    public Double getBigQuerySnapshotExpirationDays() {
        return bigQuerySnapshotExpirationDays;
    }

    public String getBackupProject() {
        return backupProject;
    }

    public String getBigQuerySnapshotStorageDataset() {
        return bigQuerySnapshotStorageDataset;
    }

    public String getGcsSnapshotStorageLocation() {
        return gcsSnapshotStorageLocation;
    }

    public GCSSnapshotFormat getGcsExportFormat() {
        return gcsExportFormat;
    }

    public String getGcsCsvDelimiter() {
        return gcsCsvDelimiter;
    }

    public Boolean getGcsCsvExportHeader() {
        return gcsCsvExportHeader;
    }

    public Boolean getGcsUseAvroLogicalTypes() {
        return gcsUseAvroLogicalTypes;
    }

    public BackupConfigSource getConfigSource() {
        return configSource;
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

    public static BackupPolicy fromJson(String jsonStr) {
        // Parse JSON as map and build the fields while applying validation
        Gson gson = new Gson();
        Map<String, String> jsonMap = gson.fromJson(jsonStr, Map.class);

        return fromMap(jsonMap);
    }

    @Override
    public String toString() {
        return "BackupPolicy{" +
                "cron='" + cron + '\'' +
                ", method=" + method +
                ", timeTravelOffsetDays=" + timeTravelOffsetDays +
                ", bigQuerySnapshotExpirationDays=" + bigQuerySnapshotExpirationDays +
                ", backupProject='" + backupProject + '\'' +
                ", bigQuerySnapshotStorageDataset='" + bigQuerySnapshotStorageDataset + '\'' +
                ", gcsSnapshotStorageLocation='" + gcsSnapshotStorageLocation + '\'' +
                ", gcsExportFormat=" + gcsExportFormat +
                ", gcsCsvDelimiter='" + gcsCsvDelimiter + '\'' +
                ", gcsCsvExportHeader=" + gcsCsvExportHeader +
                ", gcsUseAvroLogicalTypes=" + gcsUseAvroLogicalTypes +
                ", configSource=" + configSource +
                ", lastBackupAt=" + lastBackupAt +
                ", lastBqSnapshotStorageUri='" + lastBqSnapshotStorageUri + '\'' +
                ", lastGcsSnapshotStorageUri='" + lastGcsSnapshotStorageUri + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        BackupPolicy that = (BackupPolicy) o;
        return Objects.equal(cron, that.cron) && method == that.method
                && timeTravelOffsetDays == that.timeTravelOffsetDays
                && Objects.equal(bigQuerySnapshotExpirationDays, that.bigQuerySnapshotExpirationDays)
                && Objects.equal(backupProject, that.backupProject)
                && Objects.equal(bigQuerySnapshotStorageDataset, that.bigQuerySnapshotStorageDataset)
                && Objects.equal(gcsSnapshotStorageLocation, that.gcsSnapshotStorageLocation)
                && gcsExportFormat == that.gcsExportFormat && Objects.equal(gcsCsvDelimiter, that.gcsCsvDelimiter)
                && Objects.equal(gcsCsvExportHeader, that.gcsCsvExportHeader)
                && Objects.equal(gcsUseAvroLogicalTypes, that.gcsUseAvroLogicalTypes)
                && configSource == that.configSource && Objects.equal(lastBackupAt, that.lastBackupAt)
                && Objects.equal(lastBqSnapshotStorageUri, that.lastBqSnapshotStorageUri)
                && Objects.equal(lastGcsSnapshotStorageUri, that.lastGcsSnapshotStorageUri);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(cron, method, timeTravelOffsetDays, bigQuerySnapshotExpirationDays, backupProject,
                bigQuerySnapshotStorageDataset, gcsSnapshotStorageLocation, gcsExportFormat, gcsCsvDelimiter,
                gcsCsvExportHeader, gcsUseAvroLogicalTypes, configSource, lastBackupAt, lastBqSnapshotStorageUri,
                lastGcsSnapshotStorageUri);
    }

    /**
     * tagTemplateId is required.
     * tagName is optional. It's used to update an existing Tag on DataCatalog.
     *
     * @param tagTemplateId
     * @param tagName
     * @return
     */
    public Tag toDataCatalogTag(String tagTemplateId, String tagName) {

        Tag.Builder tagBuilder = Tag.newBuilder()
                .setTemplate(tagTemplateId);

        // required: cron
        tagBuilder.putFields(DataCatalogBackupPolicyTagFields.backup_cron.toString(),
                TagField.newBuilder().setStringValue(cron).build());

        // required: backup method
        tagBuilder.putFields(DataCatalogBackupPolicyTagFields.backup_method.toString(),
                TagField.newBuilder().setEnumValue(
                        TagField.EnumValue.newBuilder().setDisplayName(
                                method.getText())
                                .build())
                        .build());

        // required: time travel
        tagBuilder.putFields(DataCatalogBackupPolicyTagFields.backup_time_travel_offset_days.toString(),
                TagField.newBuilder().setEnumValue(
                        TagField.EnumValue.newBuilder().setDisplayName(
                                timeTravelOffsetDays.getText())
                                .build())
                        .build());

        // required: backup project
        TagField backupProjectField = TagField.newBuilder().setStringValue(backupProject).build();
        tagBuilder.putFields(DataCatalogBackupPolicyTagFields.backup_project.toString(), backupProjectField);

        // optional: bq snapshot expiration
        if (bigQuerySnapshotExpirationDays != null) {
            tagBuilder.putFields(DataCatalogBackupPolicyTagFields.bq_snapshot_expiration_days.toString(),
                    TagField.newBuilder().setDoubleValue(bigQuerySnapshotExpirationDays).build());
        }

        // optional: bq snapshot dataset
        if (bigQuerySnapshotStorageDataset != null) {
            tagBuilder.putFields(DataCatalogBackupPolicyTagFields.bq_snapshot_storage_dataset.toString(),
                    TagField.newBuilder().setStringValue(bigQuerySnapshotStorageDataset).build());
        }

        // optional: gcs snapshot storage location
        if (gcsSnapshotStorageLocation != null) {
            tagBuilder.putFields(DataCatalogBackupPolicyTagFields.gcs_snapshot_storage_location.toString(),
                    TagField.newBuilder().setStringValue(gcsSnapshotStorageLocation).build());
        }

        // optional: gcs export format
        if (gcsExportFormat != null) {
            tagBuilder.putFields(DataCatalogBackupPolicyTagFields.gcs_snapshot_format.toString(),
                    TagField.newBuilder().setEnumValue(
                            TagField.EnumValue.newBuilder().setDisplayName(
                                    gcsExportFormat.toString())
                                    .build())
                            .build());
        }

        // optional: gcs csv field delimiter
        if (gcsCsvDelimiter != null) {
            tagBuilder.putFields(
                    DataCatalogBackupPolicyTagFields.gcs_csv_delimiter.toString(),
                    TagField.newBuilder().setStringValue(gcsCsvDelimiter).build());
        }

        // optional: gcs_csv_export_header
        if (gcsCsvExportHeader != null) {
            tagBuilder.putFields(
                    DataCatalogBackupPolicyTagFields.gcs_csv_export_header.toString(),
                    TagField.newBuilder().setBoolValue(gcsCsvExportHeader).build());
        }

        // optional: gcs csv field delimiter
        if (gcsUseAvroLogicalTypes != null) {
            tagBuilder.putFields(
                    DataCatalogBackupPolicyTagFields.gcs_avro_use_logical_types.toString(),
                    TagField.newBuilder().setBoolValue(gcsUseAvroLogicalTypes).build());
        }

        // required: config source
        tagBuilder.putFields(DataCatalogBackupPolicyTagFields.config_source.toString(),
                TagField.newBuilder().setEnumValue(
                        TagField.EnumValue.newBuilder().setDisplayName(
                                configSource.toString())
                                .build())
                        .build());

        // optional: last backup at
        if (lastBackupAt != null) {
            tagBuilder.putFields(DataCatalogBackupPolicyTagFields.last_backup_at.toString(),
                    TagField.newBuilder().setTimestampValue(
                            com.google.protobuf.Timestamp.newBuilder()
                                    .setSeconds(lastBackupAt.getSeconds())
                                    .setNanos(lastBackupAt.getNanos())
                                    .build())
                            .build());
        }

        // optional: last bq snapshot uri
        if (lastBqSnapshotStorageUri != null) {
            tagBuilder.putFields(DataCatalogBackupPolicyTagFields.last_bq_snapshot_storage_uri.toString(),
                    TagField.newBuilder().setStringValue(lastBqSnapshotStorageUri).build());
        }

        // optional: last gcs snapshot uri
        if (lastGcsSnapshotStorageUri != null) {
            tagBuilder.putFields(DataCatalogBackupPolicyTagFields.last_gcs_snapshot_storage_uri.toString(),
                    TagField.newBuilder().setStringValue(lastGcsSnapshotStorageUri).build());
        }

        if (tagName != null) {
            tagBuilder.setName(tagName);
        }

        return tagBuilder.build();
    }

    // used to parse from json map (fallback policies) or data catalog tags (backup
    // policies)
    public static BackupPolicy fromMap(Map<String, String> tagTemplate) throws IllegalArgumentException {

        // parse required fields
        String cron = Utils.getOrFail(tagTemplate, DataCatalogBackupPolicyTagFields.backup_cron.toString());

        BackupMethod method = BackupMethod.fromString(
                Utils.getOrFail(tagTemplate, DataCatalogBackupPolicyTagFields.backup_method.toString()));

        TimeTravelOffsetDays timeTravelOffsetDays = TimeTravelOffsetDays.fromString(
                Utils.getOrFail(tagTemplate,
                        DataCatalogBackupPolicyTagFields.backup_time_travel_offset_days.toString()));

        String backupProject = Utils.getOrFail(tagTemplate, DataCatalogBackupPolicyTagFields.backup_project.toString());

        // config source is not required in the fallback policies. It defaults to SYSTEM
        // if not present
        String configSourceStr = tagTemplate.getOrDefault(
                DataCatalogBackupPolicyTagFields.config_source.toString(),
                null);

        BackupConfigSource configSource = configSourceStr == null ? BackupConfigSource.SYSTEM
                : BackupConfigSource.fromString(configSourceStr);

        BackupPolicyBuilder backupPolicyBuilder = new BackupPolicyBuilder(cron, method, timeTravelOffsetDays,
                configSource, backupProject);

        // parse optional fields
        // these fields might not exist in the attached tag template if not filled. Same
        // for fallback policies
        backupPolicyBuilder.setBigQuerySnapshotStorageDataset(
                tagTemplate.getOrDefault(
                        DataCatalogBackupPolicyTagFields.bq_snapshot_storage_dataset.toString(),
                        null));

        String bqSnapshotExpirationDaysStr = tagTemplate.getOrDefault(
                DataCatalogBackupPolicyTagFields.bq_snapshot_expiration_days.toString(),
                null);
        backupPolicyBuilder.setBigQuerySnapshotExpirationDays(
                bqSnapshotExpirationDaysStr == null ? null : Double.parseDouble(bqSnapshotExpirationDaysStr));

        // parse optional GCS snapshot settings
        backupPolicyBuilder.setGcsSnapshotStorageLocation(
                tagTemplate.getOrDefault(
                        DataCatalogBackupPolicyTagFields.gcs_snapshot_storage_location.toString(),
                        null));

        String gcsSnapshotFormatStr = tagTemplate.getOrDefault(
                DataCatalogBackupPolicyTagFields.gcs_snapshot_format.toString(),
                null);
        backupPolicyBuilder.setGcsExportFormat(
                gcsSnapshotFormatStr == null ? null : GCSSnapshotFormat.valueOf(gcsSnapshotFormatStr));

        backupPolicyBuilder.setGcsCsvDelimiter(
                tagTemplate.getOrDefault(
                        DataCatalogBackupPolicyTagFields.gcs_csv_delimiter.toString(),
                        null));

        // if optional boolean values are not provided, set them to null and not false
        String gcsCsvExportHeaderStr = tagTemplate.getOrDefault(
                DataCatalogBackupPolicyTagFields.gcs_csv_export_header.toString(),
                null);
        backupPolicyBuilder.setGcsCsvExportHeader(
                gcsCsvExportHeaderStr == null ? null : Boolean.valueOf(gcsCsvExportHeaderStr));

        // if optional boolean values are not provided, set them to null and not false
        String gcsAvroUseLogicalAvroTypeStr = tagTemplate.getOrDefault(
                DataCatalogBackupPolicyTagFields.gcs_avro_use_logical_types.toString(),
                null);

        backupPolicyBuilder.setGcsUseAvroLogicalTypes(
                gcsAvroUseLogicalAvroTypeStr == null ? null : Boolean.valueOf(gcsAvroUseLogicalAvroTypeStr));

        String lastBackupAtStr = tagTemplate.getOrDefault(
                DataCatalogBackupPolicyTagFields.last_backup_at.toString(),
                null);
        backupPolicyBuilder.setLastBackupAt(
                lastBackupAtStr == null ? null : Timestamp.parseTimestamp(lastBackupAtStr));

        backupPolicyBuilder.setLastBqSnapshotStorageUri(
                tagTemplate.getOrDefault(
                        DataCatalogBackupPolicyTagFields.last_bq_snapshot_storage_uri.toString(),
                        null));

        backupPolicyBuilder.setLastGcsSnapshotStorageUri(tagTemplate.getOrDefault(
                DataCatalogBackupPolicyTagFields.last_gcs_snapshot_storage_uri.toString(),
                null));

        return backupPolicyBuilder.build();
    }

    public static class BackupPolicyBuilder {

        // required
        private String cron;
        private BackupMethod method;
        private TimeTravelOffsetDays timeTravelOffsetDays;
        private BackupConfigSource configSource;
        private String backupProject;

        // optional
        private Double bigQuerySnapshotExpirationDays;
        private String bigQuerySnapshotStorageDataset;
        private String gcsSnapshotStorageLocation;
        private GCSSnapshotFormat gcsExportFormat;
        private String gcsCsvDelimiter;
        private Boolean gcsCsvExportHeader;
        private Boolean gcsUseAvroLogicalTypes;

        private Timestamp lastBackupAt;
        private String lastBqSnapshotStorageUri;
        private String lastGcsSnapshotStorageUri;

        public static BackupPolicyBuilder from(BackupPolicy backupPolicy) {
            return new BackupPolicyBuilder(
                    backupPolicy.cron,
                    backupPolicy.method,
                    backupPolicy.timeTravelOffsetDays,
                    backupPolicy.configSource,
                    backupPolicy.backupProject)
                    .setBigQuerySnapshotExpirationDays(backupPolicy.bigQuerySnapshotExpirationDays)
                    .setBigQuerySnapshotStorageDataset(backupPolicy.bigQuerySnapshotStorageDataset)
                    .setGcsSnapshotStorageLocation(backupPolicy.gcsSnapshotStorageLocation)
                    .setGcsExportFormat(backupPolicy.gcsExportFormat)
                    .setGcsCsvDelimiter(backupPolicy.gcsCsvDelimiter)
                    .setGcsCsvExportHeader(backupPolicy.gcsCsvExportHeader)
                    .setGcsUseAvroLogicalTypes(backupPolicy.gcsUseAvroLogicalTypes)
                    .setLastBackupAt(backupPolicy.lastBackupAt)
                    .setLastBqSnapshotStorageUri(backupPolicy.lastBqSnapshotStorageUri)
                    .setLastGcsSnapshotStorageUri(backupPolicy.lastGcsSnapshotStorageUri);
        }

        public BackupPolicyBuilder(String cron, BackupMethod method, TimeTravelOffsetDays timeTravelOffsetDays,
                BackupConfigSource configSource, String backupProject) {
            this.cron = cron;
            this.method = method;
            this.timeTravelOffsetDays = timeTravelOffsetDays;
            this.configSource = configSource;
            this.backupProject = backupProject;
        }

        public BackupPolicyBuilder setLastBackupAt(Timestamp lastBackupAt) {
            this.lastBackupAt = lastBackupAt;
            return this;
        }

        public BackupPolicyBuilder setLastBqSnapshotStorageUri(String lastBqSnapshotStorageUri) {
            this.lastBqSnapshotStorageUri = lastBqSnapshotStorageUri;
            return this;
        }

        public BackupPolicyBuilder setLastGcsSnapshotStorageUri(String lastGcsSnapshotStorageUri) {
            this.lastGcsSnapshotStorageUri = lastGcsSnapshotStorageUri;
            return this;
        }

        public BackupPolicyBuilder setCron(String cron) {
            this.cron = cron;
            return this;
        }

        public BackupPolicyBuilder setMethod(BackupMethod method) {
            this.method = method;
            return this;
        }

        public BackupPolicyBuilder setTimeTravelOffsetDays(TimeTravelOffsetDays timeTravelOffsetDays) {
            this.timeTravelOffsetDays = timeTravelOffsetDays;
            return this;
        }

        public BackupPolicyBuilder setConfigSource(BackupConfigSource configSource) {
            this.configSource = configSource;
            return this;
        }

        public BackupPolicyBuilder setBackupProject(String backupProject) {
            this.backupProject = backupProject;
            return this;
        }

        public BackupPolicyBuilder setBigQuerySnapshotExpirationDays(Double bigQuerySnapshotExpirationDays) {
            this.bigQuerySnapshotExpirationDays = bigQuerySnapshotExpirationDays;
            return this;
        }

        public BackupPolicyBuilder setBigQuerySnapshotStorageDataset(String bigQuerySnapshotStorageDataset) {
            this.bigQuerySnapshotStorageDataset = bigQuerySnapshotStorageDataset;
            return this;
        }

        public BackupPolicyBuilder setGcsSnapshotStorageLocation(String gcsSnapshotStorageLocation) {
            this.gcsSnapshotStorageLocation = gcsSnapshotStorageLocation;
            return this;
        }

        public BackupPolicyBuilder setGcsExportFormat(GCSSnapshotFormat gcsExportFormat) {
            this.gcsExportFormat = gcsExportFormat;
            return this;
        }

        public BackupPolicyBuilder setGcsCsvDelimiter(String gcsCsvDelimiter) {
            this.gcsCsvDelimiter = gcsCsvDelimiter;
            return this;
        }

        public BackupPolicyBuilder setGcsCsvExportHeader(Boolean gcsCsvExportHeader) {
            this.gcsCsvExportHeader = gcsCsvExportHeader;
            return this;
        }

        public BackupPolicyBuilder setGcsUseAvroLogicalTypes(Boolean gcsUseAvroLogicalTypes) {
            this.gcsUseAvroLogicalTypes = gcsUseAvroLogicalTypes;
            return this;
        }

        public BackupPolicy build() {
            return new BackupPolicy(this);
        }
    }
}
