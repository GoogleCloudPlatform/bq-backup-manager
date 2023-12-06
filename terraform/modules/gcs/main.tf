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

resource "google_storage_bucket" "gcs_flags_bucket" {
  project = var.project
  name          = var.gcs_flags_bucket_name
  # This bucket is used by the services so let's create in the same compute region
  location      = var.compute_region

  # force_destroy = true

  lifecycle_rule {
    condition {
      # Clean up old flags to save storage and GCS operations overhead
      age = 3 # days
    }
    action {
      type = "Delete"
    }
  }

  uniform_bucket_level_access = true

  labels = var.common_labels
}

resource "google_storage_bucket_iam_binding" "gcs_flags_bucket_iam_bindings" {
  bucket = google_storage_bucket.gcs_flags_bucket.name
  role = "roles/storage.objectAdmin"
  members = var.gcs_flags_bucket_admins
}


// Backup Polices

resource "google_storage_bucket" "gcs_backup_policies_bucket" {
  project = var.project
  name          = var.gcs_backup_policies_bucket_name
  # This bucket must be created in the same region that BigQuery dataset is created
  location      = var.data_region

  force_destroy = false

  uniform_bucket_level_access = true

  labels = var.common_labels
}

resource "google_storage_bucket_iam_binding" "gcs_backup_policies_iam_bindings" {
  bucket = google_storage_bucket.gcs_backup_policies_bucket.name
  role = "roles/storage.objectAdmin"
  members = var.gcs_backup_policies_bucket_admins
}

// Dispatched tables bucket

resource "google_storage_bucket" "gcs_dispatched_tables_bucket" {
  project = var.project
  name          = var.gcs_dispatcher_tracker_bucket_name
  # This bucket must be created in the same region that BigQuery dataset is created
  location      = var.data_region

  lifecycle_rule {
    condition {
      # Clean up old flags to save storage and GCS operations overhead
      age = var.gcs_dispatcher_tracker_bucket_ttl_days # days
    }
    action {
      type = "Delete"
    }
  }

  force_destroy = false

  uniform_bucket_level_access = true

  labels = var.common_labels
}

resource "google_storage_bucket_iam_binding" "gcs_dispatched_tables_iam_bindings" {
  bucket = google_storage_bucket.gcs_dispatched_tables_bucket.name
  role = "roles/storage.objectAdmin"
  members = var.gcs_dispatched_tables_bucket_admins
}