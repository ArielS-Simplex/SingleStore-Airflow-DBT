create TABLE PUBLIC.ETL_LOGS (
	ID serial not null,
	CONNECTION_ID VARCHAR(255),
	SCHEMA_NAME VARCHAR(255),
	TABLE_NAME VARCHAR(255),
	DAG_NAME VARCHAR(255),
	FILENAME_KEY VARCHAR(4000),
	EVENT_TS timestamp with time zone default CURRENT_TIMESTAMP,
	STATUS VARCHAR(255),
	ERROR_MESSAGE VARCHAR(10000),
	RECORDS INT
);

create or replace view PUBLIC.ETL_STATUS_REPORT
as
select schema_name,table_name,event_ts latest_event_time,status latest_status,error_message
from (
      select row_number() over(partition by schema_name,table_name order by event_ts desc) row_num,schema_name,table_name,event_ts,status,error_message
      from public.etl_logs
      where table_name <> 'dummy_source' --and error_message not like '%Detected as zombie%' and error_message not like '%Was the task killed externally%'
    ) etl_logs
where row_num = 1;

select *
from PUBLIC.ETL_STATUS_REPORT;
