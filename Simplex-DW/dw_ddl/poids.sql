CREATE TABLE poids.onfido_applicants(id int,
reference string,
onfido_applicant_id string,
created_at TIMESTAMP_TZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE poids.onfido_check_references(id int,
check_id int,
reference_id string,
created_at TIMESTAMP_TZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE poids.onfido_checks(id int,
applicant_id int,
onfido_check_id string,
created_at TIMESTAMP_TZ,
external_id string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE poids.onfido_reports(id int,
check_id int,
onfido_report_id string,
name string,
status string,
result string,
sub_result string,
breakdown variant,
created_at TIMESTAMP_TZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);