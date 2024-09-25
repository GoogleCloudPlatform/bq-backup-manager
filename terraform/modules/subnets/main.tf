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
/******************************************
	Subnet configuration
 *****************************************/
resource "google_compute_subnetwork" "subnetwork" {
  name                     = var.subnet_name
  ip_cidr_range            = var.subnet_ip
  region                   = var.subnet_region
  private_ip_google_access = var.subnet_private_access
  network                  = var.network_name
  project                  = var.project_id
  description              = var.description

  log_config {
    aggregation_interval = try(var.subnet_flow_logs_interval, "INTERVAL_5_SEC")
    flow_sampling        = try(var.subnet_flow_logs_sampling, "0.5")
    metadata             = try(var.subnet_flow_logs_metadata, "INCLUDE_ALL_METADATA")
    filter_expr          = try(var.subnet_flow_logs_filter_expr, "true")
  }

  purpose = var.purpose
  role    = var.role
}