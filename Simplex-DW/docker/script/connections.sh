
airflow connections export connections.json

if airflow connections delete "postgres_playground";
then airflow connections add    "postgres_playground" --conn-type "postgres" --conn-login "simplexcc" --conn-password "YTB2qhc_epf2dnx!tnf" --conn-host "data-utils-db.cfpbdczfqghb.eu-west-1.rds.amazonaws.com" --conn-port 5432 --conn-schema 'sample_data'
else airflow connections add    "postgres_playground" --conn-type "postgres" --conn-login "simplexcc" --conn-password "YTB2qhc_epf2dnx!tnf" --conn-host "data-utils-db.cfpbdczfqghb.eu-west-1.rds.amazonaws.com" --conn-port 5432 --conn-schema 'sample_data'
fi

if airflow connections delete "postgres_default";
then airflow connections add    "postgres_default"    --conn-type "postgres" --conn-login "simplexcc" --conn-password "xxxx" --conn-host "data-utils-db.cfpbdczfqghb.eu-west-1.rds.amazonaws.com" --conn-port 5432 --conn-schema 'sample_data'
else airflow connections add    "postgres_default"    --conn-type "postgres" --conn-login "simplexcc" --conn-password "xxxx" --conn-host "data-utils-db.cfpbdczfqghb.eu-west-1.rds.amazonaws.com" --conn-port 5432 --conn-schema 'sample_data'
fi

if airflow connections delete "data-utils-db";
then airflow connections add    "data-utils-db"    --conn-type "postgres" --conn-login "simplexcc" --conn-password "xxxx" --conn-host "data-utils-db.cfpbdczfqghb.eu-west-1.rds.amazonaws.com" --conn-port 5432 --conn-schema 'simplex_cc'
else airflow connections add    "data-utils-db"    --conn-type "postgres" --conn-login "simplexcc" --conn-password "xxxx" --conn-host "data-utils-db.cfpbdczfqghb.eu-west-1.rds.amazonaws.com" --conn-port 5432 --conn-schema 'simplex_cc'
fi

if airflow connections delete "snowflake_conn";
then airflow connections add    "snowflake_conn"     --conn-type "snowflake" --conn-login "amit.niazov@nuvei.com"  --conn-password "Snowflake123" --conn-host "aja13247.us-east-1.snowflakecomputing.com"  --conn-extra '{"extra__snowflake__account": "aja13247", "extra__snowflake__aws_access_key_id": "", "extra__snowflake__aws_secret_access_key": "", "extra__snowflake__database": "simplex_raw_stage", "extra__snowflake__region": "us-east-1", "extra__snowflake__warehouse": "COMPUTE_BI"}'
else airflow connections add    "snowflake_conn"     --conn-type "snowflake" --conn-login "amit.niazov@nuvei.com"  --conn-password "Snowflake123" --conn-host "aja13247.us-east-1.snowflakecomputing.com"  --conn-extra '{"extra__snowflake__account": "aja13247", "extra__snowflake__aws_access_key_id": "", "extra__snowflake__aws_secret_access_key": "", "extra__snowflake__database": "simplex_raw_stage", "extra__snowflake__region": "us-east-1", "extra__snowflake__warehouse": "COMPUTE_BI"}'
fi

if airflow connections delete "aws_conn";
then airflow connections add "aws_conn"  --conn-type "aws"  --conn-extra '{"region_name":"eu-west-1"}'
else airflow connections add "aws_conn"  --conn-type "aws"  --conn-extra '{"region_name":"eu-west-1"}'
fi

if airflow connections delete "aws_default";
then airflow connections add "aws_default"  --conn-type "aws" --conn-extra '{"region_name":"eu-west-1"}'
else airflow connections add "aws_default"  --conn-type "aws" --conn-extra '{"region_name":"eu-west-1"}'
fi

if connections delete "data-transformation";
then airflow connections add  data-transformation --conn-type HTTP --conn-host "http://207.232.36.155" --conn-port "8581"
else airflow connections add  data-transformation --conn-type HTTP --conn-host "http://207.232.36.155" --conn-port "8581"
fi

#if airflow connections delete "postgres_default";
#then airflow connections add    "postgres_default"    --conn-type "postgres" --conn-login "amitniazov" --conn-password "xxxx" --conn-host "read-db.simplex.com" --conn-port 5432 --conn-schema 'simplex_cc'
#else airflow connections add    "postgres_default"    --conn-type "postgres" --conn-login "amitniazov" --conn-password "xxxx" --conn-host "read-db.simplex.com" --conn-port 5432 --conn-schema 'simplex_cc'
#fi