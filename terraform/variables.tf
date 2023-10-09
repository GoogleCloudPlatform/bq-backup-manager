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

variable "application_name" {
  type = string
  default = "bq_backup_manager"
}

variable "project" {
  type = string
}

variable "compute_region" {
  type = string
}

variable "data_region" {
  type = string
}

variable "bigquery_dataset_name" {
  type = string
  default = "bq_backup_manager"
}



variable "sa_dispatcher" {
  type = string
  default = "dispatcher"
}


variable "sa_dispatcher_tasks" {
  type = string
  default = "dispatcher-tasks"
}

variable "sa_configurator" {
  type = string
  default = "configurator"
}

variable "sa_configurator_tasks" {
  type = string
  default = "configurator-tasks"
}

variable "sa_snapshoter_bq" {
  type = string
  default = "snapshoter-bq"
}

variable "sa_snapshoter_bq_tasks" {
  type = string
  default = "snapshoter-bq-tasks"
}

variable "sa_snapshoter_gcs" {
  type = string
  default = "snapshoter-gcs"
}

variable "sa_snapshoter_gcs_tasks" {
  type = string
  default = "snapshoter-gcs-tasks"
}

variable "sa_tagger" {
  type = string
  default = "tagger"
}

variable "sa_tagger_tasks" {
  type = string
  default = "tagger-tasks"
}

variable "log_sink_name" {
  type = string
  default = "sc_bigquery_log_sink"
}


variable "dispatcher_service_name" {
  type = string
  default = "s1-dispatcher"
}

variable "configurator_service_name" {
  type = string
  default = "s2-configurator"
}

variable "snapshoter_bq_service_name" {
  type = string
  default = "s3-snapshoter-bq"
}

variable "snapshoter_gcs_service_name" {
  type = string
  default = "s3-snapshoter-gcs"
}

variable "tagger_service_name" {
  type = string
  default = "s4-tagger"
}


variable "dispatcher_pubsub_topic" {
  type = string
  default = "dispatcher_topic"
}

variable "dispatcher_pubsub_sub" {
  type = string
  default = "dispatcher_push_sub"
}

variable "configurator_pubsub_topic" {
  type = string
  default = "configurator_topic"
}

variable "configurator_pubsub_sub" {
  type = string
  default = "configurator_push_sub"
}

variable "snapshoter_bq_pubsub_topic" {
  type = string
  default = "snapshoter_bq_topic"
}

variable "snapshoter_bq_pubsub_sub" {
  type = string
  default = "snapshoter_bq_push_sub"
}

variable "snapshoter_gcs_pubsub_topic" {
  type = string
  default = "snapshoter_gcs_topic"
}

variable "snapshoter_gcs_pubsub_sub" {
  type = string
  default = "snapshoter_gcs_push_sub"
}

variable "tagger_pubsub_topic" {
  type = string
  default = "tagger_topic"
}

variable "tagger_pubsub_sub" {
  type = string
  default = "tagger_push_sub"
}

variable "gcs_flags_bucket_name" {
  type = string
  default = "bq-backup-manager-flags"
}

variable "gcs_backup_policies_bucket_name" {
  type = string
  default = "bq-backup-manager-policies"
}

# Images
variable "dispatcher_service_image" {
  type = string
}

variable "configurator_service_image" {
  type = string
}

variable "snapshoter_bq_service_image" {
  type = string
}

variable "snapshoter_gcs_service_image" {
  type = string
}

variable "tagger_service_image" {
  type = string
}

variable "terraform_service_account" {
  type = string
  description = "service account used by terraform to deploy to GCP"
}

# Dispatcher settings.
variable "dispatcher_service_timeout_seconds" {
  description = "Max period for the cloud run service to complete a request. Otherwise, it terminates with HTTP 504 and NAK to PubSub (retry)"
  type = number
  # Dispatcher might need relatively long time to process large BigQuery scan scopes
  default = 60 * 60 # 60mins
}

variable "dispatcher_subscription_ack_deadline_seconds" {
  description = "This value is the maximum time after a subscriber receives a message before the subscriber should acknowledge the message. If it timeouts without ACK PubSub will retry the message."
  type = number
  // This should be higher than the service_timeout_seconds to avoid retrying messages that are still processing
  // range is 10 to 600
  default = 600 # 10m
}

variable "dispatcher_subscription_message_retention_duration" {
  description = "How long to retain unacknowledged messages in the subscription's backlog"
  type = string
  # In case of unexpected problems we want to avoid a buildup that re-trigger functions (e.g. Tagger issuing unnecessary BQ queries)
  # min value must be at least equal to the ack_deadline_seconds
  # Dispatcher should have the shortest retention possible because we want to avoid retries (on the app level as well)
  default = "600s" # 10m
}

# configurator settings.
variable "configurator_service_timeout_seconds" {
  description = "Max period for the cloud run service to complete a request. Otherwise, it terminates with HTTP 504 and NAK to PubSub (retry)"
  type = number
  // this should be lower than subscription_ack_deadline_seconds to avoid retrying messages that are still processing
  default = 300 # 5m
}

variable "configurator_subscription_ack_deadline_seconds" {
  description = "This value is the maximum time after a subscriber receives a message before the subscriber should acknowledge the message. If it timeouts without ACK PubSub will retry the message."
  type = number
  // This should be higher than the service_timeout_seconds to avoid retrying messages that are still processing
  // range [10,600] seconds
  default = 420 # 7m
}

variable "configurator_subscription_message_retention_duration" {
  description = "How long to retain unacknowledged messages in the subscription's backlog"
  type = string
  # In case of unexpected problems we want to avoid a buildup that re-trigger functions (e.g. Service issuing unnecessary API Calls)
  # It also sets how long should we keep trying to process one run
  # min value must be at least equal to the ack_deadline_seconds
  # configurator should have a relatively long retention to handle runs with large number of tables.
  default = "86400s" # 24h
}

# snapshoter_bq settings.
variable "snapshoter_bq_service_timeout_seconds" {
  description = "Max period for the cloud run service to complete a request. Otherwise, it terminates with HTTP 504 and NAK to PubSub (retry)"
  type = number
  // this should be lower than subscription_ack_deadline_seconds to avoid retrying messages that are still processing
  default = 300 # 5m
}

variable "snapshoter_bq_subscription_ack_deadline_seconds" {
  description = "This value is the maximum time after a subscriber receives a message before the subscriber should acknowledge the message. If it timeouts without ACK PubSub will retry the message."
  type = number
  // This should be higher than the service_timeout_seconds to avoid retrying messages that are still processing
  // range [10,600] seconds
  default = 420 # 7m
}

variable "snapshoter_bq_subscription_message_retention_duration" {
  description = "How long to retain unacknowledged messages in the subscription's backlog"
  type = string
  # In case of unexpected problems we want to avoid a buildup that re-trigger functions (e.g. Service issuing unnecessary API Calls)
  # It also sets how long should we keep trying to process one run
  # min value must be at least equal to the ack_deadline_seconds
  # snapshoter should have a relatively long retention to handle runs with large number of tables.
  default = "86400s" # 24h
}

# snapshoter_gcs settings.
variable "snapshoter_gcs_service_timeout_seconds" {
  description = "Max period for the cloud run service to complete a request. Otherwise, it terminates with HTTP 504 and NAK to PubSub (retry)"
  type = number
  // GCS snapshoter might take relatively long time for export jobs
  // this should be lower than subscription_ack_deadline_seconds to avoid retrying messages that are still processing
  default = 540 # 9m
}

variable "snapshoter_gcs_subscription_ack_deadline_seconds" {
  description = "This value is the maximum time after a subscriber receives a message before the subscriber should acknowledge the message. If it timeouts without ACK PubSub will retry the message."
  type = number
  // This should be higher than the service_timeout_seconds to avoid retrying messages that are still processing
  // range [10,600] seconds
  default = 600 # 10m
}

variable "snapshoter_gcs_subscription_message_retention_duration" {
  description = "How long to retain unacknowledged messages in the subscription's backlog"
  type = string
  # In case of unexpected problems we want to avoid a buildup that re-trigger functions (e.g. Service issuing unnecessary API Calls)
  # It also sets how long should we keep trying to process one run
  # min value must be at least equal to the ack_deadline_seconds
  # snapshoter should have a relatively long retention to handle runs with large number of tables.
  default = "86400s" # 24h
}

# Tagger settings.
variable "tagger_service_timeout_seconds" {
  description = "Max period for the cloud run service to complete a request. Otherwise, it terminates with HTTP 504 and NAK to PubSub (retry)"
  type = number
  // this should be lower than subscription_ack_deadline_seconds to avoid retrying messages that are still processing
  default = 540 # 9m
}

variable "tagger_subscription_ack_deadline_seconds" {
  description = "This value is the maximum time after a subscriber receives a message before the subscriber should acknowledge the message. If it timeouts without ACK PubSub will retry the message."
  type = number
  // This should be higher than the service_timeout_seconds to avoid retrying messages that are still processing
  // range [10,600] seconds
  default = 600 # 10m
}

variable "tagger_subscription_message_retention_duration" {
  description = "How long to retain unacknowledged messages in the subscription's backlog"
  type = string
  # In case of unexpected problems we want to avoid a buildup that re-trigger functions (e.g. Tagger issuing unnecessary BQ queries)
  # It also sets how long should we keep trying to process one run
  # min value must be at least equal to the ack_deadline_seconds
  # Tagger should have a relatively long retention to handle runs with large number of tables.
  default = "86400s" # 24h
}

variable "schedulers" {
  type = list(object({
    name = string,
    cron = string,
    payload = object({
      is_force_run = bool,
      is_dry_run = bool,
      folders_include_list = list(number),
      projects_include_list = list(string),
      projects_exclude_list = list(string),
      datasets_include_list = list(string),
      datasets_exclude_list = list(string),
      tables_include_list = list(string),
      tables_exclude_list = list(string),
    })
  }))
}

variable "fallback_policy" {
  type = object({
    default_policy = map(string),
    folder_overrides = map(map(string)),
    project_overrides = map(map(string)),
    dataset_overrides = map(map(string)),
    table_overrides = map(map(string)),
  })
}

// make sure that you include all projects in this list while calling /scripts/prepare_backup_storage_projects.sh to grant terraform SA permissions to deploy resources there
variable "additional_backup_operation_projects" {
  type = list(string)
  default = []
  description = "Projects were backup operations will run but not defined in the fallback policy (e.g. in Tag policies). Used to deploy required resources on these projects."
}

#########################################################################################################
#   Networking & VPC SC Variables
#########################################################################################################

variable "vpc_network_name" {
  type = string
  default = "bq-backup-manager-vpc"
  description = "VPC Network name"
}

variable "vpc_network_description" {
  type = string
  default = "VPC Network for BigQuery Backup Manager"
  description = "VPC network description"
}

variable "vpc_network_routing_mode" {
  type = string
  default = "REGIONAL"
  description = "VPC network routing mode"
}

variable "subnet_name" {
  type = string
  default = "bq-backup-manager-subnet"
  description = "Subnet name"
}

variable "subnet_description" {
  type = string
  default = "Subnet for BigQuery Backup Manager"
  description = "Subnet description"
}

variable "subnet_range" {
  type = string
  default = "10.0.0.0/28"
  description = "Subnet range"
}

variable "firewall_rule_egress_deny_all_name" {
  type = string
  default = "backup-manager-egress-deny-all"
  description = "Firewall rule name for denying all egress"
}

variable "firewall_rule_egress_deny_all_description" {
  type = string
  default = "Deny all egress traffic"
  description = "Firewall rule description for denying all egress"
}

variable "firewall_rule_egress_allow_restricted_name" {
  type = string
  default = "backup-manager-egress-allow-restricted"
  description = "Firewall rule name for allowing restricted APIs access"
}

variable "firewall_rule_egress_allow_restricted_description" {
  type = string
  default = "Allow egress traffic only from restricted apis"
  description = "Firewall rule description for allowing restricted APIs access"
}

variable "dns_googleapis_name" {
  type = string
  default = "backup-manager-googleapis-dns"
  description = "Cloud DNS name for googleapis"
}

#variable "dns_cloudrun_name" {
#  type = string
#  default = "backup-manager-cloud-run-dns"
#  description = "Cloud DNS name for Cloud Run"
#}

variable "serverless_vpc_connector_name" {
  type = string
  default = "backup-manager-svpc-conn"
  description = "Serverless VPC connector name"
}

variable "serverless_vpc_connector_machine_type" {
  type = string
  default = "e2-micro"
  description = "Serverless VPC connector machine type"
}





