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
variable "project" {type = string}
variable "sa_dispatcher" {type = string}
variable "sa_dispatcher_tasks" {type = string}
variable "sa_configurator" {type = string}
variable "sa_configurator_tasks" {type = string}
variable "sa_snapshoter_bq" {type = string}
variable "sa_snapshoter_gcs" {type = string}
variable "sa_snapshoter_bq_tasks" {type = string}
variable "sa_snapshoter_gcs_tasks" {type = string}
variable "sa_tagger" {type = string}
variable "sa_tagger_tasks"{type = string}