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

resource "google_logging_project_sink" "bigquery-logging-sink" {
  name = var.log_sink_name
  destination = "bigquery.googleapis.com/projects/${var.project}/datasets/${var.dataset}"
  filter = "resource.type=cloud_run_revision jsonPayload.global_app=${var.application_name}"
  # Use a unique writer (creates a unique service account used for writing)
  unique_writer_identity = true
  bigquery_options {
    use_partitioned_tables = true
  }
}