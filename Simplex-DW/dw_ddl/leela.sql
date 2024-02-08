CREATE TABLE Leela.leela_requests(id int,
inserted_at TIMESTAMP_NTZ,
payment_id int,
caller_request_id string,
checkpoint_name string,
request_time TIMESTAMP_NTZ,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE leela.processor_routing_decisions(id int,
inserted_at TIMESTAMP_NTZ,
leela_requests_id int,
leela_caller_request_id string,
sorted_processors string,
deciding_rule_name string,
sorting_rule_decisions variant,
routing_params variant, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE leela.policy_decisions(id int,
inserted_at TIMESTAMP_NTZ,
request_id int,
decision string,
actions string,
reason string,
message string,
code string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE leela.nibbler_processor_sorting_decisions(id int,
inserted_at TIMESTAMP_TZ,
leela_requests_id int,
analytic_code_version string,
decision string,
reason string,
analytic_reason string,
message string,
code string,
execution_env string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE leela.nibbler_decisions(id int,
inserted_at TIMESTAMP_NTZ,
request_id int,
response variant,
decision_engine string,
decision_engine_strategy string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE leela.policy_service_response(id int,
inserted_at TIMESTAMP_NTZ,
leela_request_id int,
payment_id int,
request_id string,
request variant,
response variant,
processor_blocking_rules variant,
allowed_processors string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);