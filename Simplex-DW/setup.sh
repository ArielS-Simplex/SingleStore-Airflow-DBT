suffix=$1
sed -e 's/"config"/"config_'$suffix'"/; s/"datalake_flow"/"datalake_flow_'$suffix'"/' dags/datalake_flow.py > dags/datalake_flow_${suffix}.py
aws s3 cp dags/datalake_flow_${suffix}.py s3://splx-prod-airflow/dags/datalake_flow_${suffix}.py
