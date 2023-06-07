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


output "sa_dispatcher_email" {
  value = google_service_account.sa_dispatcher.email
}

output "sa_configurator_email" {
  value = google_service_account.sa_configurator.email
}

output "sa_snapshoter_bq_email" {
  value = google_service_account.sa_snapshoter_bq.email
}

output "sa_snapshoter_gcs_email" {
  value = google_service_account.sa_snapshoter_gcs.email
}

output "sa_tagger_email" {
  value = google_service_account.sa_tagger.email
}

output "sa_dispatcher_tasks_email" {
  value = google_service_account.sa_dispatcher_tasks.email
}

output "sa_configurator_tasks_email" {
  value = google_service_account.sa_configurator_tasks.email
}

output "sa_snapshoter_bq_tasks_email" {
  value = google_service_account.sa_snapshoter_bq_tasks.email
}

output "sa_snapshoter_gcs_tasks_email" {
  value = google_service_account.sa_snapshoter_gcs_tasks.email
}

output "sa_tagger_tasks_email" {
  value = google_service_account.sa_tagger_tasks.email
}





