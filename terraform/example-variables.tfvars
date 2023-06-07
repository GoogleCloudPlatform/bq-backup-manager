
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
project        = "<host project name>"
compute_region = "<gcp region>"
data_region    = "<gcp region including multi-regions eu|us>"

cloud_scheduler_account = "service-<project number>@gcp-sa-cloudscheduler.iam.gserviceaccount.com"

terraform_service_account = "bq-backup-mgr-terraform@<host project name>.iam.gserviceaccount.com"

dispatcher_service_image     = "<compute region>-docker.pkg.dev/<host project>/<docker repo>/bqsm-dispatcher-service:latest"
configurator_service_image   = "<compute region>-docker.pkg.dev/<host project>/<docker repo>/bqsm-configurator-service:latest"
snapshoter_bq_service_image  = "<compute region>-docker.pkg.dev/<host project>/<docker repo>/bqsm-snapshoter-bq-service:latest"
snapshoter_gcs_service_image = "<compute region>-docker.pkg.dev/<host project>/<docker repo>/bqsm-snapshoter-gcs-service:latest"
tagger_service_image         = "<compute region>-docker.pkg.dev/<host project>/<docker repo>/bqsm-tagger-service:latest"

schedulers = [
  {
    name    = "heart_beat"
    cron    = "0 * * * *" # hourly
    payload = {
      is_force_run = false
      is_dry_run   = false

      folders_include_list  = []
      projects_include_list = []
      projects_exclude_list = []
      datasets_include_list = []
      datasets_exclude_list = []
      tables_include_list   = []
      tables_exclude_list   = []
    }
  }
]

# Fallback policies
fallback_policy = {
  "default_policy" : {
    "backup_cron" : "0 0 */6 * * *", # every 6 hours each day
    "backup_method" : "Both",
    "backup_time_travel_offset_days" : "7",
    "backup_project" : "project name",
    # bq settings
    "bq_snapshot_expiration_days" : "15",
    "bq_snapshot_storage_dataset" : "backups",
    # gcs settings
    "gcs_snapshot_storage_location" : "gs://backups/"
    "gcs_snapshot_format" : "AVRO_SNAPPY"
    "gcs_avro_use_logical_types" : true
  },

  # if no folder overrides exists set "folder_overrides : {}"
  "folder_overrides" : {

    "<folder number>" : {
      # policy attributes
    },
  },

  # if no project overrides exists set "project_overrides : {}"
  "project_overrides" : {
    "<project name>" : {
      # policy attributes
    }
  },

  # if no dataset overrides exists set "dataset_overrides : {}"
  "dataset_overrides" : {
    "<project name>.<dataset name>" : {
      # policy attributes
    }
  },

  # if no table overrides exists set "table_overrides : {}"
  "table_overrides" : {
    "<project name>.<dataset name>.<table name>" : {
      # policy attributes
    }
  }
}
