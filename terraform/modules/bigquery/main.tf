#
#  Copyright 2023 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


######## Datasets ##############################################################

resource "google_bigquery_dataset" "results_dataset" {
  project = var.project
  location = var.region
  dataset_id = var.dataset
  labels = var.common_labels
}

# Logging BQ sink must be able to write data to logging table in the dataset
resource "google_bigquery_dataset_iam_member" "logging_sink_access" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  role = "roles/bigquery.dataEditor"
  member = var.logging_sink_sa
}

##### Tables #######################################################


resource "google_bigquery_table" "logging_table" {
  project = var.project
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  # don't change the name so that cloud logging can find it
  table_id = "run_googleapis_com_stdout"

  time_partitioning {
    type = "DAY"
    #expiration_ms = 604800000 # 7 days
  }

  schema = file("modules/bigquery/schema/run_googleapis_com_stdout.json")

  deletion_protection = true

  # labels causes Terraform to force replace the table at each deployment for some reason which we don't want to do for the log table containing history logs
  # labels = var.common_labels
}


### Monitoring Views ##################################################

resource "google_bigquery_table" "view_audit_log_by_table" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id = "v_audit_log_by_table"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query = templatefile("modules/bigquery/views/v_audit_log_by_table.tpl",
      {
        project = var.project
        dataset = var.dataset
        logging_table = google_bigquery_table.logging_table.table_id
      }
    )
  }

  labels = var.common_labels
}

resource "google_bigquery_table" "view_audit_log_by_table_grouped" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id = "v_audit_log_by_table_grouped"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query = templatefile("modules/bigquery/views/v_audit_log_by_table_grouped.tpl",
      {
        project = var.project
        dataset = var.dataset
        v_audit_log_by_table = google_bigquery_table.view_audit_log_by_table.table_id
      }
    )
  }

  labels = var.common_labels
}

resource "google_bigquery_table" "logging_view_steps" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id = "v_steps"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query = templatefile("modules/bigquery/views/v_steps.tpl",
    {
      project = var.project
      dataset = var.dataset
      logging_table = google_bigquery_table.logging_table.table_id
    }
    )
  }

  labels = var.common_labels
}

resource "google_bigquery_table" "view_service_calls" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id = "v_service_calls"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query = templatefile("modules/bigquery/views/v_service_calls.tpl",
    {
      project = var.project
      dataset = var.dataset
      logging_view_steps = google_bigquery_table.logging_view_steps.table_id
    }
    )
  }

  labels = var.common_labels
}

resource "google_bigquery_table" "view_run_summary" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id = "v_run_summary"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query = templatefile("modules/bigquery/views/v_run_summary.tpl",
    {
      project = var.project
      dataset = var.dataset
      v_unified_logging = google_bigquery_table.view_audit_log_by_table.table_id
    }
    )
  }

  labels = var.common_labels
}

resource "google_bigquery_table" "view_run_summary_counts" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id = "v_run_summary_counts"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query = templatefile("modules/bigquery/views/v_run_summary_counts.tpl",
    {
      project = var.project
      dataset = var.dataset
      v_run_summary = google_bigquery_table.view_run_summary.table_id
      v_run_duration = google_bigquery_table.view_run_duration.table_id
      v_backed_up_tables = google_bigquery_table.view_backed_up_tables.table_id
    }
    )
  }

  labels = var.common_labels
}

resource "google_bigquery_table" "view_errors_non_retryable" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id = "v_errors_non_retryable"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query = templatefile("modules/bigquery/views/v_errors_non_retryable.tpl",
    {
      project = var.project
      dataset = var.dataset
      logging_table = google_bigquery_table.logging_table.table_id
    }
    )
  }

  labels = var.common_labels
}

resource "google_bigquery_table" "view_errors_retryable" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id = "v_errors_retryable"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query = templatefile("modules/bigquery/views/v_errors_retryable.tpl",
    {
      project = var.project
      dataset = var.dataset
      logging_table = google_bigquery_table.logging_table.table_id
    }
    )
  }

  labels = var.common_labels
}

resource "google_bigquery_table" "view_tracking_id_map" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id = "v_tracking_id_to_table_map"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query = templatefile("modules/bigquery/views/v_tracking_id_to_table_map.tpl",
    {
      project = var.project
      dataset = var.dataset
      logging_table = google_bigquery_table.logging_table.table_id
    }
    )
  }

  labels = var.common_labels
}

resource "google_bigquery_table" "view_run_duration" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id = "v_run_duration"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query = templatefile("modules/bigquery/views/v_run_duration.tpl",
      {
        project = var.project
        dataset = var.dataset
        logging_table = google_bigquery_table.logging_table.table_id
      }
    )
  }

  labels = var.common_labels
}


resource "google_bigquery_table" "view_backed_up_tables" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id = "v_backed_up_tables"

  deletion_protection = false

  view {
    use_legacy_sql = false
    query = templatefile("modules/bigquery/views/v_backed_up_tables.tpl",
      {
        project = var.project
        dataset = var.dataset
        v_audit_log_by_table = google_bigquery_table.view_audit_log_by_table.table_id
      }
    )
  }

  labels = var.common_labels
}

########## External tables #####################################

resource "google_bigquery_table" "external_gcs_backup_policies" {
  dataset_id = google_bigquery_dataset.results_dataset.dataset_id
  table_id   = "ext_backup_policies"

  external_data_configuration {
    source_format = "NEWLINE_DELIMITED_JSON"
    hive_partitioning_options {
      mode = "CUSTOM" # Custom means you must encode the partition key schema within the source_uri_prefix
      source_uri_prefix = "gs://${var.gcs_backup_policies_bucket_name}/{project:STRING}/{dataset:STRING}/{table:STRING}"

    }
    source_uris = [
      "gs://${var.gcs_backup_policies_bucket_name}/*.json",
    ]
    autodetect = false # Let BigQuery try to autodetect the schema and format of the table.
    schema = file("modules/bigquery/schema/ext_backup_policies.json")
  }

  deletion_protection = false
}








