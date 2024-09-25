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
variable "project_id" {
  description = "The ID of the project where subnets will be created"
  type = string
}

variable "subnet_name" {
  description = "The name of the resource, provided by the client when initially creating the resource."
  type        = string

  validation {
    condition     = length(var.subnet_name) <= 63 && can(regex("[a-z]([-a-z0-9]*[a-z0-9])?", var.subnet_name))
    error_message = "The network_name should be less than 63 characters."
  }
}

variable "subnet_ip" {
  description = "The range of internal addresses that are owned by this subnetwork."
  type        = string
}

variable "subnet_region" {
  description = "The GCP region for this subnetwork."
  type        = string
}

variable "subnet_private_access" {
  description = "When enabled, VMs in this subnetwork without external IP addresses can access Google APIs and services by using Private Google Access."
  type        = bool
  default     = true
}

variable "network_name" {
  description = "The network this subnet belongs to. Only networks that are in the distributed mode can have subnetworks."
  type        = string
}

variable "description" {
  type        = string
  description = "An optional description of this resource. Provide this property when you create the resource. This field can be set only at resource creation time."
  default     = null
}

variable "subnet_flow_logs_interval" {
  description = "Can only be specified if VPC flow logging for this subnetwork is enabled. Toggles the aggregation interval for collecting flow logs."
  type        = string
  default     = "INTERVAL_5_SEC"
}

variable "subnet_flow_logs_sampling" {
  description = "Can only be specified if VPC flow logging for this subnetwork is enabled. The value of the field must be in [0, 1]."
  type        = string
  default     = "0.5"
}

variable "subnet_flow_logs_metadata" {
  description = "Configures whether metadata fields should be added to the reported VPC flow logs. Default value is INCLUDE_ALL_METADATA. Possible values are EXCLUDE_ALL_METADATA, INCLUDE_ALL_METADATA, and CUSTOM_METADATA."
  type        = string
  default     = "INCLUDE_ALL_METADATA"
}

variable "subnet_flow_logs_filter_expr" {
  description = "Export filter used to define which VPC flow logs should be logged, as as CEL expression."
  type        = bool
  default     = "true"
}

variable "purpose" {
  type        = string
  description = "The purpose of the resource. A subnetwork with purpose set to INTERNAL_HTTPS_LOAD_BALANCER is a user-created subnetwork that is reserved for Internal HTTP(S) Load Balancing."
  default     = null
}

variable "role" {
  type        = string
  description = "The role of subnetwork. Currently, this field is only used when purpose = INTERNAL_HTTPS_LOAD_BALANCER. The value can be set to ACTIVE or BACKUP."
  default     = null
}