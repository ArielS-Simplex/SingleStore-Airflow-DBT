CREATE TABLE leela_replica.nibbler_processor_sorting_decisions(id int,
inserted_at TIMESTAMP_TZ,
leela_requests_id int,
analytic_code_version string,
decision ARRAY
reason string,
analytic_reason string,
message string,
code string,
execution_env string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);