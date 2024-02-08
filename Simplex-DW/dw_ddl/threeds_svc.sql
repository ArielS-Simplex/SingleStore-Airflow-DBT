CREATE TABLE threeds_svc.authentication_requests(id string,
threeds_id string,
inserted_at TIMESTAMP_TZ,
version string,
threeds_provider string,
challenge_preferences string,
raw_request variant,
raw_request_cardholder_email variant,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE threeds_svc.authentication_responses(authentication_requests_id string,
threeds_id string,
inserted_at TIMESTAMP_TZ,
status string,
eci string,
authentication_value string,
ds_transaction_id string,
errors variant,
raw_response variant,
raw_response_authenticationrequest_email string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE threeds_svc.threeds(id string,
request_id string,
credit_card_id string,
created_at TIMESTAMP_TZ,
amount number(20,4),
currency_code string,
processor_name string,
merchant_id string,
requested_version string,
card_scheme string,
simplex_mid_key string,
raw_data variant,
raw_data_cardHolder_email variant,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE threeds_svc.create_pareq_requests(id string,
threeds_id string,
inserted_at TIMESTAMP_TZ,
threeds_provider string,
merchant_id string,
raw_request variant,
callback_url string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE threeds_svc.create_pareq_responses(create_pareq_request_id string,
threeds_id string,
inserted_at TIMESTAMP_TZ,
threeds_provider string,
acs_url string,
enrollment string,
enrollment_id string,
pareq string,
pareq_id string,
errors variant,
raw_response variant, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE threeds_svc.pares_validation_requests(id string,
threeds_id string,
threeds_provider string,
inserted_at TIMESTAMP_TZ,
raw_request variant, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE threeds_svc.pares_validation_responses(pares_validation_requests_id string,
threeds_id string,
inserted_at TIMESTAMP_TZ,
status string,
eci string,
xid string,
authentication_value string,
cavv_algorithm string,
signature_status string,
errors variant,
raw_response variant, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE threeds_svc.threeds_versioning_requests(
threeds_id string,
version_request_id string,
 created_at TIMESTAMP_TZ,
 dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE threeds_svc.versioning_requests(id string,
threeds_provider string,
raw_request variant,
inserted_at TIMESTAMP_TZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE threeds_svc.versioning_responses(versioning_requests_id string,
inserted_at TIMESTAMP_TZ,
version variant,
errors variant,
raw_response variant,
threeds_method_data_form string,
threeds_method_url string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);