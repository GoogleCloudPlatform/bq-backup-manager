#!/bin/bash

#
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -e

echo "enabling cloudresourcemanager .."
gcloud services enable cloudresourcemanager.googleapis.com

echo "enabling iam .."
gcloud services enable iam.googleapis.com

echo "enabling datacatalog .."
gcloud services enable datacatalog.googleapis.com

echo "enabling artifactregistry .."
gcloud services enable artifactregistry.googleapis.com

echo "enabling bigquery .."
gcloud services enable bigquery.googleapis.com

echo "enabling pubsub .."
gcloud services enable pubsub.googleapis.com

echo "enabling storage .."
gcloud services enable storage.googleapis.com

echo "enabling run .."
gcloud services enable run.googleapis.com

echo "enabling cloudbuild .."
gcloud services enable cloudbuild.googleapis.com

echo "enabling serviceusage .."
gcloud services enable serviceusage.googleapis.com

echo "enabling appengine .."
gcloud services enable appengine.googleapis.com