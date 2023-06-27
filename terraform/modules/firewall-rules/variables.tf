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
  description = "Project id of the project that holds the network."
  type        = string
}

variable "network_name" {
  description = "Name of the network this set of firewall rules applies to."
  type        = string
}

variable "name" {
  description = "Name of the resource."
  type        = string

  validation {
    condition     = length(var.name) <= 63 && can(regex("[a-z]([-a-z0-9]*[a-z0-9])?", var.name))
    error_message = "The name should be less than 63 characters."
  }
}

variable "description" {
  description = "An optional description of this resource. Provide this property when you create the resource."
  type        = string
  default     = null
}

variable "direction" {
  description = "Direction of traffic to which this firewall applies."
  type        = string
  default     = "INGRESS"
}

variable "priority" {
  description = "Priority for this rule. This is an integer between 0 and 65535, both inclusive."
  type        = number
  default     = null
}

variable "ranges" {
  description = "If ranges are specified, the firewall will apply only to traffic that has source IP address in these ranges."
  type        = list(string)
  default     = null
}

variable "source_tags" {
  description = "If source tags are specified, the firewall will apply only to traffic with source IP that belongs to a tag listed in source tags."
  type        = list(string)
  default     = null
}

variable "source_service_accounts" {
  description = "If source service accounts are specified, the firewall will apply only to traffic originating from an instance with a service account in this list."
  type        = list(string)
  default     = null
}

variable "target_tags" {
  description = "A list of instance tags indicating sets of instances located in the network that may make network connections as specified in allowed."
  type        = list(string)
  default     = null
}

variable "target_service_accounts" {
  description = "A list of service accounts indicating sets of instances located in the network that may make network connections as specified in allowed."
  type        = list(string)
  default     = null
}

variable "allow" {
  description = "The list of ALLOW rules specified by this firewall. Each rule specifies a protocol and port-range tuple that describes a permitted connection."
  type        = list(any)
  default     = null
}

variable "deny" {
  description = "The list of DENY rules specified by this firewall. Each rule specifies a protocol and port-range tuple that describes a denied connection."
  type        = list(any)
  default     = null
}

variable "metadata" {
  description = "This field denotes whether to include or exclude metadata for firewall logs. Possible values are EXCLUDE_ALL_METADATA and INCLUDE_ALL_METADATA."
  type        = string
  default     = "INCLUDE_ALL_METADATA"
}