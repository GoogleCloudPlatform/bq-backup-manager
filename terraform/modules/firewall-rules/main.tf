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

resource "google_compute_firewall" "rules" {
  name                    = var.name
  description             = var.description
  direction               = var.direction
  network                 = var.network_name
  project                 = var.project_id
  source_ranges           = var.direction == "INGRESS" ? var.ranges : null
  destination_ranges      = var.direction == "EGRESS" ? var.ranges : null
  source_tags             = var.source_tags
  source_service_accounts = var.source_service_accounts
  target_tags             = var.target_tags
  target_service_accounts = var.target_service_accounts
  priority                = var.priority

  log_config {
    metadata = var.metadata
  }

  dynamic "allow" {
    for_each = var.allow == null ? [] : var.allow
    content {
      protocol = allow.value.protocol
      ports    = try(allow.value.ports, null)
    }
  }

  dynamic "deny" {
    for_each = var.deny == null ? [] : var.deny
    content {
      protocol = deny.value.protocol
      ports    = try(deny.value.ports, null)
    }
  }
}