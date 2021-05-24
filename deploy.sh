#!/usr/bin/env bash
#
# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Need to get shell lib files ready before import them.
npm install

SOLUTION_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" -ef "$0" ]]; then
  RELATIVE_PATH="node_modules/@google-cloud"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/nodejs-common/bin/install_functions.sh"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/nodejs-common/bin/bigquery.sh"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/gmp-googleads-connector/deploy.sh"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/data-tasks-coordinator/deploy.sh"
fi

# Project namespace will be used as prefix of the name of Cloud Functions,
# Pub/Sub topics, etc.
# Default project namespace is SOLUTION_NAME.
# Note: only lowercase letters, numbers and dashes(-) are allowed.
PROJECT_NAMESPACE="da"
TIMEZONE="Australia/Sydney"

# BigQuery Dataset Id.
DATASET="firebase_predictions"

# Other
GCS_BUCKET="canva-da-bucket-test"
DATASET_LOCATION="us"

# Parameter name used by functions to load and save config.
CONFIG_FOLDER_NAME="OUTBOUND"
CONFIG_ITEMS=(
  "PROJECT_NAMESPACE"
  "GCS_BUCKET"
  "DATASET"
  "DATASET_LOCATION"
  "${CONFIG_FOLDER_NAME}"
)

# Use this to create of topics and subscriptions.
SELECTED_APIS_CODES=("CM")

#######################################
# Create a Cloud Schedular Job which target Pub/Sub.
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   Task name, a string.
#######################################
create_start_cron_job() {
  check_authentication
  quit_if_failed $?
  check_firestore_existence
  local job_name=${PROJECT_NAMESPACE}-start
  create_or_update_cloud_scheduler_for_pubsub \
    $job_name \
    "0 6 * * *" \
    "${TIMEZONE}" \
    ${PROJECT_NAMESPACE}-monitor \
    '{
       "timezone":"'"${TIMEZONE}"'",
       "partitionDay":"${yesterday}"
    }' \
    taskId=start
}

#######################################
# Create a BigQuery table for double activated users. It will create an empty
# table with 'today' as the suffix to enable the query which is based on this
# clustered tables.
# Globals:
#   DATASET
# Arguments:
#   None
#######################################
create_table(){
  (( STEP += 1 ))
  printf '%s\n' "Step ${STEP}: Creating a Bigquery table for double activated \
users..."
  local today
  today=$(date '+%Y%m%d')
  bq mk -t "${DATASET}.double_activation_users_${today}" \
gclid:STRING,timestamp_micros:INTEGER
  printf '\n'
}

# Install
DEFAULT_INSTALL_TASKS=(
  "print_welcome Double_Activation"
  check_in_cloud_shell
  prepare_dependencies
  confirm_namespace confirm_project
  check_permissions_native enable_apis
  confirm_region
  "confirm_bucket_with_location GCS_BUCKET us-central1"
  confirm_folder
  "confirm_dataset_with_location DATASET us"
  create_table
  save_config
  check_firestore_existence
  create_subscriptions
  create_sink
  deploy_tentacles
  deploy_cloud_functions_task_coordinator
  copy_sql_to_gcs
  "update_api_config ./config_api.json"
  "update_task_config ./config_task.json"
  create_start_cron_job
  "print_finished Double_Activation"
)

run_default_function "$@"
