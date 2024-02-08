#!/bin/bash
set -e
set +x

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

if [ "$ENVIRONMENT" == "prod" ];then
  #PROD
  AIRFLOW_ENVIRONMENT=airflow-prod-non-pci
  S3_BUCKET_PATH=splx-prod-airflow
  S3_PLUGINS_PATH=plugins.zip
  S3_REQUIREMENTS_PATH=requirements/requirements2_4_3.txt
elif [ "$ENVIRONMENT" == "tests" ];then
  #TESTS
  AIRFLOW_ENVIRONMENT=airflow-do-tests-non-pci
  S3_BUCKET_PATH=simplex-apt-airflow-test
  S3_PLUGINS_PATH=plugins/plugins.zip
  S3_REQUIREMENTS_PATH=requirements/requirements.txt
else
  echo "ENVIRONMENT=${ENVIRONMENT}, supported envs: prod|tests"
  exit 1
fi

aws s3 cp ${SCRIPT_DIR}/../plugins/plugins.zip s3://${S3_BUCKET_PATH}/$S3_PLUGINS_PATH
aws s3 cp ${SCRIPT_DIR}/../dags/requirements.txt s3://${S3_BUCKET_PATH}/$S3_REQUIREMENTS_PATH
