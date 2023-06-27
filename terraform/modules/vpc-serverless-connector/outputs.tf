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

output "id" {
  value = google_vpc_access_connector.connector.id
  description = "An identifier for the resource with format projects/{{project}}/locations/{{region}}/connectors/{{name}}"
}

output "state" {
  value = google_vpc_access_connector.connector.state
  description = "State of the VPC access connector."
}

output "self_link" {
  value = google_vpc_access_connector.connector.self_link
  description = "The fully qualified name of this VPC connector."
}

output "name" {
  value = google_vpc_access_connector.connector.name
  description = "The name of the VPC connector."
}

output "connector" {
  value = google_vpc_access_connector.connector
  description = "The created VPC connector resource."
}