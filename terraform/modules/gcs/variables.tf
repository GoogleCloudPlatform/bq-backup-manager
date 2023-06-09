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

variable "project" {type = string}

variable "compute_region" {type = string}

variable "data_region" {type = string}

variable "gcs_flags_bucket_name" {type = string}

variable "gcs_flags_bucket_admins" {
  type = list(string)
}

variable "gcs_backup_policies_bucket_name" {type = string}

variable "gcs_backup_policies_bucket_admins" {
  type = list(string)
}

variable "common_labels" {
  type = map(string)
}