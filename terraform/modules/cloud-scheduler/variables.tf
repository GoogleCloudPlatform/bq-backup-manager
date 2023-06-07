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
variable "project" {
  type = string
}

variable "scheduler_name" {
  type = string
}
variable "target_uri" {
  type = string
}
variable "cron_expression" {
  type = string
}

# BigQuery Scope
# Fromat:
//# {
//is_force_run = boolean
//folders_include_list = []
//projects_include_list = []
//projects_exclude_list = []
//datasets_include_list = []
//datasets_exclude_list = []
//tables_include_list = []
//tables_exclude_list = []
//}
variable "payload" {
  type = object({
    is_force_run = bool,
    is_dry_run = bool
    folders_include_list = list(number),
    projects_include_list = list(string),
    projects_exclude_list = list(string),
    datasets_include_list = list(string),
    datasets_exclude_list = list(string),
    tables_include_list = list(string),
    tables_exclude_list = list(string),
  })
}
