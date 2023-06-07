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
resource "google_cloud_scheduler_job" "scheduler_job" {
  project = var.project
  name = var.scheduler_name
  description = "CRON job to trigger BigQuery Backup Manager"
  schedule = var.cron_expression

  retry_config {
    retry_count = 0
  }

  pubsub_target {
    # topic.id is the topic's full resource name.
    topic_name = var.target_uri
    data = base64encode(jsonencode(
    {
      isForceRun = lookup(var.payload, "is_force_run"),
      isDryRun = lookup(var.payload, "is_dry_run"),
      bigQueryScope = {
        folderIncludeList = lookup(var.payload, "folders_include_list"),
        projectIncludeList = lookup(var.payload, "projects_include_list"),
        projectExcludeList = lookup(var.payload, "projects_exclude_list"),
        datasetIncludeList = lookup(var.payload, "datasets_include_list"),
        datasetExcludeList = lookup(var.payload, "datasets_exclude_list"),
        tableIncludeList = lookup(var.payload, "tables_include_list"),
        tableExcludeList = lookup(var.payload, "tables_exclude_list")
      }
    }
    ))
  }
}


