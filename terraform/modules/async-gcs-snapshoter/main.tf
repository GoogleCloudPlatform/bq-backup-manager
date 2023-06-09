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


// create a pubsub log sink in the backup project where the bq extract jobs run
resource "google_logging_project_sink" "backup_project_pubsub_sink" {
  project = var.log_project
  name = "${var.log_sink_name}_${var.host_project}"
  destination = "pubsub.googleapis.com/projects/${var.host_project}/topics/${var.pubsub_topic_name}"
  filter = "resource.type=bigquery_resource protoPayload.serviceData.jobCompletedEvent.eventName=extract_job_completed protoPayload.serviceData.jobCompletedEvent.job.jobConfiguration.labels.app=${var.application_name}"
  # Use a unique writer (creates a unique service account used for writing)
  unique_writer_identity = true
}

// grant access to the sink service account to publish messages to pubsub
resource "google_pubsub_topic_iam_member" "sa_topic_publisher" {
  project = var.host_project
  topic = "projects/${var.host_project}/topics/${var.pubsub_topic_name}"
  role = "roles/pubsub.publisher"
  member = google_logging_project_sink.backup_project_pubsub_sink.writer_identity
}