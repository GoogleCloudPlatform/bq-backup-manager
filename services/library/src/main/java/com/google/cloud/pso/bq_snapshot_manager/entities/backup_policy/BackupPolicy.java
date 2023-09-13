package com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy;

import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BackupPolicy {

  @SerializedName("backup_cron")
  private final String cron;

  @SerializedName("backup_method")
  private final BackupMethod method;

  @SerializedName("backup_time_travel_offset_days")
  private final TimeTravelOffsetDays timeTravelOffsetDays;

  @SerializedName("bq_snapshot_expiration_days")
  private final Double bigQuerySnapshotExpirationDays;

  @SerializedName("backup_storage_project")
  private final String backupStorageProject;

  @SerializedName("backup_operation_project")
  private final String backupOperationProject;

  @SerializedName("bq_snapshot_storage_dataset")
  private final String bigQuerySnapshotStorageDataset;

  @SerializedName("gcs_snapshot_storage_location")
  private final String gcsSnapshotStorageLocation;

  @SerializedName("gcs_snapshot_format")
  private final GCSSnapshotFormat gcsExportFormat;

  @SerializedName("gcs_csv_delimiter")
  private final String gcsCsvDelimiter;

  @SerializedName("gcs_csv_export_header")
  private final Boolean gcsCsvExportHeader;

  @SerializedName("gcs_avro_use_logical_types")
  private final Boolean gcsUseAvroLogicalTypes;

  @SerializedName("config_source")
  private final BackupConfigSource configSource;

  public BackupPolicy(BackupPolicy.BackupPolicyBuilder builder) throws IllegalArgumentException {

    List<BackupPolicyFields> missingFields = validate(builder);

    if (!missingFields.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Backup policy is invalid for backup method '%s'. The following fields are missing %s",
              builder.method, missingFields));
    }

    this.cron = builder.cron;
    this.method = builder.method;
    this.timeTravelOffsetDays = builder.timeTravelOffsetDays;
    this.bigQuerySnapshotExpirationDays = builder.bigQuerySnapshotExpirationDays;
    this.backupStorageProject = builder.backupStorageProject;
    this.backupOperationProject = builder.backupOperationProject;
    this.bigQuerySnapshotStorageDataset = builder.bigQuerySnapshotStorageDataset;
    this.gcsSnapshotStorageLocation = builder.gcsSnapshotStorageLocation;
    this.gcsExportFormat = builder.gcsExportFormat;
    this.gcsCsvDelimiter = builder.gcsCsvDelimiter;
    this.gcsCsvExportHeader = builder.gcsCsvExportHeader;
    this.gcsUseAvroLogicalTypes = builder.gcsUseAvroLogicalTypes;
    this.configSource = builder.configSource;
  }

  public static List<BackupPolicyFields> validate(BackupPolicyBuilder builder) {
    // validate that all required fields are provided depending on the backup method
    List<BackupPolicyFields> missingRequired = new ArrayList<>();
    List<BackupPolicyFields> missingOptional = new ArrayList<>();

    if (builder.cron == null) {
      missingRequired.add(BackupPolicyFields.backup_cron);
    }
    if (builder.method == null) {
      missingRequired.add(BackupPolicyFields.backup_method);
    }
    if (builder.timeTravelOffsetDays == null) {
      missingRequired.add(BackupPolicyFields.backup_time_travel_offset_days);
    }
    if (builder.configSource == null) {
      missingRequired.add(BackupPolicyFields.config_source);
    }
    if (builder.backupStorageProject == null) {
      missingRequired.add(BackupPolicyFields.backup_storage_project);
    }

    // if required params are missing return and don't continue with other checks
    if (!missingRequired.isEmpty()) {
      return missingRequired;
    }

    if (builder.method.equals(BackupMethod.BIGQUERY_SNAPSHOT)
        || builder.method.equals(BackupMethod.BOTH)) {
      if (builder.bigQuerySnapshotStorageDataset == null) {
        missingOptional.add(BackupPolicyFields.bq_snapshot_storage_dataset);
      }
      if (builder.bigQuerySnapshotExpirationDays == null) {
        missingOptional.add(BackupPolicyFields.bq_snapshot_expiration_days);
      }
    }

    // check for GCS params
    if (builder.method.equals(BackupMethod.GCS_SNAPSHOT)
        || builder.method.equals(BackupMethod.BOTH)) {

      String format =
          builder.gcsExportFormat == null
              ? null
              : GCSSnapshotFormat.getFormatAndCompression(builder.gcsExportFormat).x();

      if (builder.gcsExportFormat == null) {
        missingOptional.add(BackupPolicyFields.gcs_snapshot_format);
      }
      if (builder.gcsSnapshotStorageLocation == null) {
        missingOptional.add(BackupPolicyFields.gcs_snapshot_storage_location);
      }
      // check required fields for CSV exports
      if (format != null && format.equals("CSV")) {
        if (builder.gcsCsvDelimiter == null) {
          missingOptional.add(BackupPolicyFields.gcs_csv_delimiter);
        }
        if (builder.gcsCsvExportHeader == null) {
          missingOptional.add(BackupPolicyFields.gcs_csv_export_header);
        }
      }
      // check for required fields for avro export
      if (format != null && format.equals("AVRO")) {
        if (builder.gcsUseAvroLogicalTypes == null) {
          missingOptional.add(BackupPolicyFields.gcs_avro_use_logical_types);
        }
      }
    }

    return missingOptional;
  }

  public Map<String, String> toMap() {
    Map<String, String> fields = new HashMap<>();
    // required fields
    fields.put(BackupPolicyFields.backup_cron.toString(), cron);
    fields.put(BackupPolicyFields.backup_method.toString(), method.getText());
    fields.put(
        BackupPolicyFields.backup_time_travel_offset_days.toString(),
        timeTravelOffsetDays.getText());
    fields.put(BackupPolicyFields.backup_storage_project.toString(), backupStorageProject);
    fields.put(BackupPolicyFields.config_source.toString(), configSource.getText());
    fields.put(BackupPolicyFields.backup_storage_project.toString(), backupStorageProject);
    fields.put(BackupPolicyFields.config_source.toString(), configSource.getText());

    // optional fields
    if (this.getBackupOperationProject() != null) {
      fields.put(
          BackupPolicyFields.backup_operation_project.toString(), this.getBackupOperationProject());
    }

    if (this.getBigQuerySnapshotExpirationDays() != null) {
      fields.put(
          BackupPolicyFields.bq_snapshot_expiration_days.toString(),
          this.getBigQuerySnapshotExpirationDays().toString());
    }

    if (this.getBigQuerySnapshotStorageDataset() != null) {
      fields.put(
          BackupPolicyFields.bq_snapshot_storage_dataset.toString(),
          this.getBigQuerySnapshotStorageDataset());
    }

    if (this.getGcsSnapshotStorageLocation() != null) {
      fields.put(
          BackupPolicyFields.gcs_snapshot_storage_location.toString(),
          this.getGcsSnapshotStorageLocation());
    }

    if (this.getGcsExportFormat() != null) {
      fields.put(
          BackupPolicyFields.gcs_snapshot_format.toString(), this.getGcsExportFormat().toString());
    }

    if (this.getGcsCsvDelimiter() != null) {
      fields.put(BackupPolicyFields.gcs_csv_delimiter.toString(), this.getGcsCsvDelimiter());
    }

    if (this.getGcsCsvExportHeader() != null) {
      fields.put(
          BackupPolicyFields.gcs_csv_export_header.toString(),
          this.getGcsCsvExportHeader().toString());
    }

    if (this.getGcsUseAvroLogicalTypes() != null) {
      fields.put(
          BackupPolicyFields.gcs_avro_use_logical_types.toString(),
          this.getGcsUseAvroLogicalTypes().toString());
    }

    return fields;
  }

  // used to parse from json map (fallback policies) or data catalog tags (backup policies)
  public static BackupPolicy fromMap(Map<String, String> fieldsMap)
      throws IllegalArgumentException {

    // parse required fields
    String cron = Utils.getOrFail(fieldsMap, BackupPolicyFields.backup_cron.toString());

    BackupMethod method =
        BackupMethod.fromString(
            Utils.getOrFail(fieldsMap, BackupPolicyFields.backup_method.toString()));

    TimeTravelOffsetDays timeTravelOffsetDays =
        TimeTravelOffsetDays.fromString(
            Utils.getOrFail(
                fieldsMap, BackupPolicyFields.backup_time_travel_offset_days.toString()));

    String backupStorageProject =
        Utils.getOrFail(fieldsMap, BackupPolicyFields.backup_storage_project.toString());

    // config source is not required in the fallback policies. It defaults to SYSTEM if not present
    String configSourceStr =
        fieldsMap.getOrDefault(BackupPolicyFields.config_source.toString(), null);

    BackupConfigSource configSource =
        configSourceStr == null
            ? BackupConfigSource.SYSTEM
            : BackupConfigSource.fromString(configSourceStr);

    BackupPolicyBuilder backupPolicyBuilder =
        new BackupPolicyBuilder(
            cron, method, timeTravelOffsetDays, configSource, backupStorageProject);

    // parse optional fields
    // these fields might not exist in the attached tag template if not filled. Same for fallback
    // policies

    backupPolicyBuilder.setBackupOperationProject(
        fieldsMap.getOrDefault(BackupPolicyFields.backup_operation_project.toString(), null));

    backupPolicyBuilder.setBigQuerySnapshotStorageDataset(
        fieldsMap.getOrDefault(BackupPolicyFields.bq_snapshot_storage_dataset.toString(), null));

    String bqSnapshotExpirationDaysStr =
        fieldsMap.getOrDefault(BackupPolicyFields.bq_snapshot_expiration_days.toString(), null);
    backupPolicyBuilder.setBigQuerySnapshotExpirationDays(
        bqSnapshotExpirationDaysStr == null
            ? null
            : Double.parseDouble(bqSnapshotExpirationDaysStr));

    // parse optional GCS snapshot settings
    backupPolicyBuilder.setGcsSnapshotStorageLocation(
        fieldsMap.getOrDefault(BackupPolicyFields.gcs_snapshot_storage_location.toString(), null));

    String gcsSnapshotFormatStr =
        fieldsMap.getOrDefault(BackupPolicyFields.gcs_snapshot_format.toString(), null);
    backupPolicyBuilder.setGcsExportFormat(
        gcsSnapshotFormatStr == null ? null : GCSSnapshotFormat.valueOf(gcsSnapshotFormatStr));

    backupPolicyBuilder.setGcsCsvDelimiter(
        fieldsMap.getOrDefault(BackupPolicyFields.gcs_csv_delimiter.toString(), null));

    // if optional boolean values are not provided, set them to null and not false
    String gcsCsvExportHeaderStr =
        fieldsMap.getOrDefault(BackupPolicyFields.gcs_csv_export_header.toString(), null);
    backupPolicyBuilder.setGcsCsvExportHeader(
        gcsCsvExportHeaderStr == null ? null : Boolean.valueOf(gcsCsvExportHeaderStr));

    // if optional boolean values are not provided, set them to null and not false
    String gcsAvroUseLogicalAvroTypeStr =
        fieldsMap.getOrDefault(BackupPolicyFields.gcs_avro_use_logical_types.toString(), null);

    backupPolicyBuilder.setGcsUseAvroLogicalTypes(
        gcsAvroUseLogicalAvroTypeStr == null
            ? null
            : Boolean.valueOf(gcsAvroUseLogicalAvroTypeStr));

    return backupPolicyBuilder.build();
  }

  public static BackupPolicy fromJson(String jsonStr) {
    // Parse JSON as map and build the fields while applying validation
    Gson gson = new Gson();
    Map<String, String> jsonMap = gson.fromJson(jsonStr, Map.class);

    return fromMap(jsonMap);
  }

  public String toJson() {
    return new Gson().toJson(toMap());
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

  public String getBackupStorageProject() {
    return backupStorageProject;
  }

  public String getBackupOperationProject() {
    return backupOperationProject;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BackupPolicy that = (BackupPolicy) o;
    return Objects.equal(cron, that.cron)
        && method == that.method
        && timeTravelOffsetDays == that.timeTravelOffsetDays
        && Objects.equal(bigQuerySnapshotExpirationDays, that.bigQuerySnapshotExpirationDays)
        && Objects.equal(backupStorageProject, that.backupStorageProject)
        && Objects.equal(backupOperationProject, that.backupOperationProject)
        && Objects.equal(bigQuerySnapshotStorageDataset, that.bigQuerySnapshotStorageDataset)
        && Objects.equal(gcsSnapshotStorageLocation, that.gcsSnapshotStorageLocation)
        && gcsExportFormat == that.gcsExportFormat
        && Objects.equal(gcsCsvDelimiter, that.gcsCsvDelimiter)
        && Objects.equal(gcsCsvExportHeader, that.gcsCsvExportHeader)
        && Objects.equal(gcsUseAvroLogicalTypes, that.gcsUseAvroLogicalTypes)
        && configSource == that.configSource;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        cron,
        method,
        timeTravelOffsetDays,
        bigQuerySnapshotExpirationDays,
        backupStorageProject,
        backupOperationProject,
        bigQuerySnapshotStorageDataset,
        gcsSnapshotStorageLocation,
        gcsExportFormat,
        gcsCsvDelimiter,
        gcsCsvExportHeader,
        gcsUseAvroLogicalTypes,
        configSource);
  }

  @Override
  public String toString() {
    return "BackupPolicy{"
        + "cron='"
        + cron
        + '\''
        + ", method="
        + method
        + ", timeTravelOffsetDays="
        + timeTravelOffsetDays
        + ", bigQuerySnapshotExpirationDays="
        + bigQuerySnapshotExpirationDays
        + ", backupStorageProject='"
        + backupStorageProject
        + '\''
        + ", backupOperationProject='"
        + backupOperationProject
        + '\''
        + ", bigQuerySnapshotStorageDataset='"
        + bigQuerySnapshotStorageDataset
        + '\''
        + ", gcsSnapshotStorageLocation='"
        + gcsSnapshotStorageLocation
        + '\''
        + ", gcsExportFormat="
        + gcsExportFormat
        + ", gcsCsvDelimiter='"
        + gcsCsvDelimiter
        + '\''
        + ", gcsCsvExportHeader="
        + gcsCsvExportHeader
        + ", gcsUseAvroLogicalTypes="
        + gcsUseAvroLogicalTypes
        + ", configSource="
        + configSource
        + '}';
  }

  public static class BackupPolicyBuilder {

    // required
    private String cron;
    private BackupMethod method;
    private TimeTravelOffsetDays timeTravelOffsetDays;
    private BackupConfigSource configSource;
    // project where the backup will be stored
    private String backupStorageProject;

    // project where operations will run (e.g. snapshot, export, etc)
    private String backupOperationProject;

    // optional
    private Double bigQuerySnapshotExpirationDays;
    private String bigQuerySnapshotStorageDataset;
    private String gcsSnapshotStorageLocation;
    private GCSSnapshotFormat gcsExportFormat;
    private String gcsCsvDelimiter;
    private Boolean gcsCsvExportHeader;
    private Boolean gcsUseAvroLogicalTypes;

    public static BackupPolicy.BackupPolicyBuilder from(BackupPolicy backupPolicy) {
      return new BackupPolicy.BackupPolicyBuilder(
              backupPolicy.cron,
              backupPolicy.method,
              backupPolicy.timeTravelOffsetDays,
              backupPolicy.configSource,
              backupPolicy.backupStorageProject)
          .setBigQuerySnapshotExpirationDays(backupPolicy.bigQuerySnapshotExpirationDays)
          .setBackupOperationProject(backupPolicy.backupOperationProject)
          .setBigQuerySnapshotStorageDataset(backupPolicy.bigQuerySnapshotStorageDataset)
          .setGcsSnapshotStorageLocation(backupPolicy.gcsSnapshotStorageLocation)
          .setGcsExportFormat(backupPolicy.gcsExportFormat)
          .setGcsCsvDelimiter(backupPolicy.gcsCsvDelimiter)
          .setGcsCsvExportHeader(backupPolicy.gcsCsvExportHeader)
          .setGcsUseAvroLogicalTypes(backupPolicy.gcsUseAvroLogicalTypes);
    }

    public BackupPolicyBuilder(
        String cron,
        BackupMethod method,
        TimeTravelOffsetDays timeTravelOffsetDays,
        BackupConfigSource configSource,
        String backupStorageProject) {
      this.cron = cron;
      this.method = method;
      this.timeTravelOffsetDays = timeTravelOffsetDays;
      this.configSource = configSource;
      this.backupStorageProject = backupStorageProject;
    }

    public BackupPolicy.BackupPolicyBuilder setBackupOperationProject(
        String backupOperationProject) {
      this.backupOperationProject = backupOperationProject;
      return this;
    }

    public BackupPolicy.BackupPolicyBuilder setBigQuerySnapshotExpirationDays(
        Double bigQuerySnapshotExpirationDays) {
      this.bigQuerySnapshotExpirationDays = bigQuerySnapshotExpirationDays;
      return this;
    }

    public BackupPolicy.BackupPolicyBuilder setBigQuerySnapshotStorageDataset(
        String bigQuerySnapshotStorageDataset) {
      this.bigQuerySnapshotStorageDataset = bigQuerySnapshotStorageDataset;
      return this;
    }

    public BackupPolicy.BackupPolicyBuilder setGcsSnapshotStorageLocation(
        String gcsSnapshotStorageLocation) {
      this.gcsSnapshotStorageLocation = gcsSnapshotStorageLocation;
      return this;
    }

    public BackupPolicy.BackupPolicyBuilder setGcsExportFormat(GCSSnapshotFormat gcsExportFormat) {
      this.gcsExportFormat = gcsExportFormat;
      return this;
    }

    public BackupPolicy.BackupPolicyBuilder setGcsCsvDelimiter(String gcsCsvDelimiter) {
      this.gcsCsvDelimiter = gcsCsvDelimiter;
      return this;
    }

    public BackupPolicy.BackupPolicyBuilder setGcsCsvExportHeader(Boolean gcsCsvExportHeader) {
      this.gcsCsvExportHeader = gcsCsvExportHeader;
      return this;
    }

    public BackupPolicy.BackupPolicyBuilder setGcsUseAvroLogicalTypes(
        Boolean gcsUseAvroLogicalTypes) {
      this.gcsUseAvroLogicalTypes = gcsUseAvroLogicalTypes;
      return this;
    }

    public BackupPolicy build() {
      return new BackupPolicy(this);
    }
  }
}
