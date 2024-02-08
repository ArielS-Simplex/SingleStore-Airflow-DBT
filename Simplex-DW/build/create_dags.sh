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
  S3_PLUGINS_PATH=plugins.zip
  S3_REQUIREMENTS_PATH=requirements/requirements.txt
else
  echo "ENVIRONMENT=${ENVIRONMENT}, supported envs: prod|tests"
  exit 1
fi

# Upload all the dags except for datalake_flow
DAGS="${SCRIPT_DIR}/../dags/*"

for file in $DAGS
do
  basename_file=`basename $file`
  if [[ $basename_file != @(datalake_flow.py|pg_data_sync.py)  && $basename_file == *.py ]]; then
    echo "uploading file ${SCRIPT_DIR}/../dags/${basename_file} to s3 bucket: "
    aws s3 cp ${SCRIPT_DIR}/../dags/${basename_file} s3://${S3_BUCKET_PATH}/dags/
  fi
done

# Upload all the scripts
SCRIPTS="${SCRIPT_DIR}/../dags/scripts/*"

for script in SCRIPTS
do
  echo "uploading dags/scripts to s3 bucket: "
  aws s3 cp ${SCRIPT_DIR}/../dags/scripts s3://${S3_BUCKET_PATH}/dags/scripts --recursive
done

#Upload config directories - add new configs here!
CONFIGS="${SCRIPT_DIR}/../config"
aws s3 cp ${SCRIPT_DIR}/../config s3://${S3_BUCKET_PATH}/config/ \
          --exclude "cache" \
          --exclude "data" \
          --exclude "logs" \
          --exclude "var_libs" \
          --recursive

# ------New Build a dag for each config and upload to s3----------------
CONFIGS="${SCRIPT_DIR}/../config/pg_data_sync/*"
for file in $CONFIGS
do
    basename_file=`basename $file`
    echo
    # if file in config/ start with "config_"
    if [[ $basename_file == config_* ]]; then
        suffix=`echo $basename_file | sed 's/config_//g' | sed 's/.json//g'`

        echo "creating & updating file: $file, with basename: $basename_file to ${SCRIPT_DIR}/../dags/pg_data_sync_${suffix}.py"
        rm -rf ${SCRIPT_DIR}/../dags/pg_data_sync_${suffix}.py
        sed -e 's/"config"/"config_'$suffix'"/; s/"pg_data_sync"/"pg_data_sync_'$suffix'"/' ${SCRIPT_DIR}/../dags/pg_data_sync.py > ${SCRIPT_DIR}/../dags/pg_data_sync_${suffix}.py
        echo "uploading file ${SCRIPT_DIR}/../dags/pg_data_sync_${suffix}.py to s3 bucket: "
        aws s3 cp ${SCRIPT_DIR}/../dags/pg_data_sync_${suffix}.py s3://${S3_BUCKET_PATH}/dags/
    else
        echo "file: $file, with basename: $basename_file, not start with config_!"
    fi
done


# Update MWAA environment if needed
latest_plugins_version=$(docker run -v ~/.aws:/root/.aws amazon/aws-cli s3api list-object-versions --bucket $S3_BUCKET_PATH --prefix $S3_PLUGINS_PATH --max-items 1 | grep -Eo -m 1 '"VersionId"[^,]*' | grep -Eo '[^:]*$' | tr -d '"' | sed -e 's/^[[:space:]]*//')
echo "latest_plugins_version=$latest_plugins_version"
running_plugins_version=$(docker run -v ~/.aws:/root/.aws amazon/aws-cli mwaa get-environment --name $AIRFLOW_ENVIRONMENT | grep -Eo -m 1 '"PluginsS3ObjectVersion"[^,]*' | grep -Eo '[^:]*$' | tr -d '"' | sed -e 's/^[[:space:]]*//')
echo "running_plugins_version=$running_plugins_version"
latest_requirements_version=$(docker run -v ~/.aws:/root/.aws amazon/aws-cli s3api list-object-versions --bucket $S3_BUCKET_PATH --prefix $S3_REQUIREMENTS_PATH --max-items 1 | grep -Eo -m 1 '"VersionId"[^,]*' | grep -Eo '[^:]*$' | tr -d '"' | sed -e 's/^[[:space:]]*//')
echo "latest_requirements_version=$latest_requirements_version"
running_requirements_version=$(docker run -v ~/.aws:/root/.aws amazon/aws-cli mwaa get-environment --name $AIRFLOW_ENVIRONMENT | grep -Eo -m 1 '"RequirementsS3ObjectVersion"[^,]*' | grep -Eo '[^:]*$' | tr -d '"' | sed -e 's/^[[:space:]]*//' )
echo "running_requirements_version=$running_requirements_version"

if [ "$latest_plugins_version" == "$running_plugins_version" ] && [ "$latest_requirements_version" == "$running_requirements_version" ]; then
    echo "Airflow environment $AIRFLOW_ENVIRONMENT is already updated with latest plugins and requirements"
else
    echo "Updating airflow environment $AIRFLOW_ENVIRONMENT"
    docker run -v ~/.aws:/root/.aws amazon/aws-cli mwaa update-environment --name $AIRFLOW_ENVIRONMENT --plugins-s3-object-version "$latest_plugins_version" --requirements-s3-object-version "$latest_requirements_version"
fi

#docker run -v ~/.aws:/root/.aws amazon/aws-cli mwaa create-environment --name DWAirflow22 --plugins-s3-object-version "$latest_plugins_version" --requirements-s3-object-version "$latest_requirements_version" --airflow-version "2.2.2" --dag-s3-path "dags" --environment-class "mw1.medium" --execution-role-arn "arn:aws:role:::AmazonMWAA-DWAirflow-mqBD6q" --source-bucket-arn "arn:aws:s3:::splx-prod-airflow" --plugins-s3-path "plugins.zip" --requirements-s3-path "dags/requirements.txt" --min-workers "2" --max-workers "25"
