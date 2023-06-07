#   Copyright 2023 Google LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
### Create Tag Template

resource "google_data_catalog_tag_template" "snapshot_tag_template" {
  tag_template_id = "bq_backup_manager_template"
  project = var.project
  region = var.region
  display_name = "BigQuery Backup Manager"

  fields {
    field_id = "config_source"
    display_name = "Configuration Source"
    order = 15
    type {
      enum_type {
        allowed_values {
          display_name = "MANUAL"
        }
        allowed_values {
          display_name = "SYSTEM"
        }
      }
    }
    is_required = true
  }

  fields {
    field_id = "backup_cron"
    display_name = "Cron expression for backup frequency"
    order = 14
    type {
      primitive_type = "STRING"
    }
    is_required = true
  }

  fields {
    field_id = "backup_time_travel_offset_days"
    display_name = "Number of days in the past where the backup is taken relative to NOW"
    order = 13
    type {
      enum_type {
        allowed_values {
          display_name = "0"
        }
        allowed_values {
          display_name = "1"
        }
        allowed_values {
          display_name = "2"
        }
        allowed_values {
          display_name = "3"
        }
        allowed_values {
          display_name = "4"
        }
        allowed_values {
          display_name = "5"
        }
        allowed_values {
          display_name = "6"
        }
        allowed_values {
          display_name = "7"
        }
      }
    }
    is_required = true
  }

  fields {
    field_id = "backup_method"
    display_name = "How to backup this table"
    order = 12
    type {
      enum_type {
        allowed_values {
          display_name = "BigQuery Snapshot"
        }
        allowed_values {
          display_name = "GCS Snapshot"
        }
        allowed_values {
          display_name = "Both"
        }
      }
    }
    is_required = true
  }

  fields {
    field_id = "backup_project"
    display_name = "Project to run snapshot and export operations and store their results"
    order = 11
    type {
      primitive_type = "STRING"
    }
    is_required = true
  }

  fields {
    field_id = "bq_snapshot_storage_dataset"
    display_name = "BigQuery - Dataset where the snapshot is stored"
    order = 10
    type {
      primitive_type = "STRING"
    }
    is_required = false
  }

  fields {
    field_id = "bq_snapshot_expiration_days"
    display_name = "BigQuery - Snapshot retention period in days"
    order = 9
    type {
      primitive_type = "DOUBLE"
    }
    is_required = false
  }

  fields {
    field_id = "gcs_snapshot_storage_location"
    display_name = "GCS - Parent path to store all table snapshots"
    order = 8
    type {
      primitive_type = "STRING"
    }
    is_required = false
  }

  fields {
    field_id = "gcs_snapshot_format"
    display_name = "GCS - Export format and compression"
    order = 7
    type {
      enum_type {
        allowed_values {
          display_name = "CSV"
        }
        allowed_values {
          display_name = "CSV_GZIP"
        }
        allowed_values {
          display_name = "JSON"
        }
        allowed_values {
          display_name = "JSON_GZIP"
        }
        allowed_values {
          display_name = "AVRO"
        }
        allowed_values {
          display_name = "AVRO_DEFLATE"
        }
        allowed_values {
          display_name = "AVRO_SNAPPY"
        }
        allowed_values {
          display_name = "PARQUET"
        }
        allowed_values {
          display_name = "PARQUET_SNAPPY"
        }
        allowed_values {
          display_name = "PARQUET_GZIP"
        }
      }
    }
    is_required = false
  }

  fields {
    field_id = "gcs_csv_delimiter"
    display_name = "GCS - CSV field delimiter"
    order = 6
    type {
      primitive_type = "STRING"
    }
    is_required = false
  }

  fields {
    field_id = "gcs_csv_export_header"
    display_name = "GCS - CSV export header row"
    order = 5
    type {
      primitive_type = "BOOL"
    }
    is_required = false
  }

  fields {
    field_id = "gcs_avro_use_logical_types"
    display_name = "GCS - Use Avro logical types"
    order = 4
    type {
      primitive_type = "BOOL"
    }
    is_required = false
  }

  fields {
    field_id = "last_backup_at"
    display_name = "Read-Only - Timestamp of the latest backup taken"
    order = 3
    type {
      primitive_type = "TIMESTAMP"
    }
    is_required = false
  }

  fields {
    field_id = "last_gcs_snapshot_storage_uri"
    display_name = "Read-Only - Last GCS snapshot location"
    order = 2
    type {
      primitive_type = "STRING"
    }
    is_required = false
  }

  fields {
    field_id = "last_bq_snapshot_storage_uri"
    display_name = "Read-Only - Last BQ snapshot location"
    order = 1
    type {
      primitive_type = "STRING"
    }
    is_required = false
  }

  // deleting the tag template will delete all configs attached to tables
  force_delete = true
}

resource "google_data_catalog_tag_template_iam_member" "tag_template_user" {
  count = length(var.tagTemplateUsers)
  tag_template = google_data_catalog_tag_template.snapshot_tag_template.name
  role = "roles/datacatalog.tagTemplateUser"
  member = "serviceAccount:${var.tagTemplateUsers[count.index]}"
}