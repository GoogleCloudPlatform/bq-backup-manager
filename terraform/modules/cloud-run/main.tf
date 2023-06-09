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

locals {
  timestamp = formatdate("YYYY-MM-DD-hh:mm:ss", timestamp())
}

resource "google_cloud_run_service" "service" {
  project = var.project
  name = var.service_name
  location = var.region

  template {
    spec {
      timeout_seconds = var.timeout_seconds
      service_account_name = var.service_account_email

      container_concurrency = var.max_requests_per_container

      containers {
        image = var.service_image

        resources {
          limits = {
            "memory": var.max_memory
            "cpu": var.max_cpu
          }
        }

        dynamic env {
          for_each = var.environment_variables
          content {
            name = env.value["name"]
            value = env.value["value"]
          }
        }


        # Hack to force terraform to re-deploy this service (e.g. update latest image)
        env {
          name = "TERRAFORM_UPDATED_AT"
          value = local.timestamp
        }
      }
    }
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale"  = var.max_containers
        "run.googleapis.com/vpc-access-connector" = var.vpc_access_connector
        "run.googleapis.com/vpc-access-egress"    = "all-traffic"
      }
    }
  }

  metadata {
    annotations = {
      "run.googleapis.com/ingress" : "internal"
    }
    labels = var.common_labels
  }

  traffic {
    percent = 100
    latest_revision = true
  }
}

### Service X Tasks SA must be able to invoke Service X  ####
resource "google_cloud_run_service_iam_member" "sa_invoker" {

  project = google_cloud_run_service.service.project
  location = google_cloud_run_service.service.location
  service = google_cloud_run_service.service.name
  role = "roles/run.invoker"
  member = "serviceAccount:${var.invoker_service_account_email}"
}