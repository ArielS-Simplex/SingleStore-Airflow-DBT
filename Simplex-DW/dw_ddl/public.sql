CREATE TABLE public.decisions (
       id int,
       application_name string,
       analytic_code_version string,
       payment_id int,
       executed_at TIMESTAMP_NTZ,
       decision string,
       variables variant,
       created_at TIMESTAMP_NTZ,
       batch_id int,
       reason string,
       request_id string,
       r_payment_id int,
       reason_code string,
       updated_at TIMESTAMP_TZ,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);

CREATE TABLE public.email_normalized (
id int,
original_email string,
email string,
created_at TIMESTAMP_NTZ,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.enrich_binlist (
            id int,
       inserted_at TIMESTAMP_TZ,
       bin string,
       response_data variant,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.enrich_maxmind (
id int,
inserted_at TIMESTAMP_TZ,
data variant,
request_data variant,
context variant,
hash_key string,
version int,
device_id string,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.execution_quotes (
       payment_id int,
       quote_id string,
       created_at TIMESTAMP_TZ,
       quote_type string,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.maxmind_triggers (
       id int,
       inserted_at TIMESTAMP_TZ,
       r_payment_id int,
       enrich_maxmind_id int,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.partner_end_users (
id int,
partner_id int,
partner_account_id string,
verified_at TIMESTAMP_NTZ,
account_tier string,
account_tier_updated_at TIMESTAMP_NTZ,
email string,
phone string,
country string,
state string,
date_of_birth TIMESTAMP_NTZ,
gender string,
two_fa_enabled boolean,
two_fa_updated_at TIMESTAMP_NTZ,
passport_details variant,
utility_bill_details variant,
created_at TIMESTAMP_TZ,
updated_at TIMESTAMP_TZ,
signup_login variant,
kyc_updated_at TIMESTAMP_NTZ,
token_created_at string,
cap_data variant,
past_tx_data variant,
first_logins variant,
last_logins variant,
last_verified_email string,
signup_login_mm_country string,
signup_login_mm_state string,
first_logins_mm_country string,
first_logins_mm_state string,
last_logins_mm_country string,
last_logins_mm_state string,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);

CREATE TABLE public.partners (
       id int,
       name string,
       created_at TIMESTAMP_TZ,
       redirect_back_to string,
       service_type string,
       min_amount number(20,4),
       max_amount number(20,4),
       support_email string,
       verification_request_email string,
       config variant,
       policy variant,
       updated_at TIMESTAMP_TZ,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.payment_credit_cards (
       id int,
       payment_id int,
       credit_card_id string,
       created_at TIMESTAMP_TZ,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.payment_events_log (
       id int,
       payment_id int,
       name string,
       created_at TIMESTAMP_TZ,
       event_id string,
       partner_id int,
       consumed_at TIMESTAMP_NTZ,
       payment variant,
       payment_partner_end_user_id string,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.payment_fees (
       payment_id int,
       fee_request_id int,
       fee_type string,
       model string,
       strategy_name string,
       amount number(20,4),
       currency string,
       created_at TIMESTAMP_TZ,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.payment_methods (
       id int,
       payment_id int,
       payment_method string,
       created_at TIMESTAMP_TZ,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.payment_raw_requests (
       id int,
       request_data variant,
       payment_id int,
       inserted_at TIMESTAMP_TZ,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.payments (
       id int,
       payment_id string,
       partner_end_user_id int,
       btc_address string,
       created_at TIMESTAMP_TZ,
       status int,
       partner_login variant,
       simplex_login variant,
       simplex_end_user_id int,
       updated_at TIMESTAMP_TZ,
       currency string,
       handling_user_id int,
       total_amount number(20,4),
       country string,
       state string,
       authorization_request_id string,
       handling_at TIMESTAMP_NTZ,
       order_id string,
       is_tampering_attempt boolean,
       payment_request_body variant,
       validation_request_body variant,
       payment_request_body_email string,
       validation_request_body_email string,
       chargeback_at TIMESTAMP_NTZ,
       chargeback_reason string,
       processor_id int,
       fee number(20,4),
       quote_id string,
       pay_to_partner_id int,
       affiliate_fee number(20,4),
       original_http_ref_url string,
       email string,
       phone string,
       update_event string,
       update_event_inserted_at TIMESTAMP_NTZ,
       date_of_birth TIMESTAMP_NTZ,
       crypto_currency string,
       crypto_currency_address string,
       total_amount_usd number(20,4),
       usd_rate number(20,4),
       conversion_time TIMESTAMP_NTZ,
       cc_identifier string,
       cc_expiry_identifier string,
       bin string,
       mid string,
       crypto_currency_address_tag string,
       affiliate_site_referrer string,
       crypto_currency_address_memo string,
       simplex_login_ip_country string,
       simplex_login_ip_state   string,
       partner_login_ip_country  string,
       partner_login_ip_state   string,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);


CREATE TABLE public.proc_requests (
       id int,
       created_at TIMESTAMP_TZ,
       updated_at TIMESTAMP_TZ,
       payment_id int,
       request_id string,
       raw_response variant,
       raw_response_profile_email string,
       raw_response_customer_email string,
       tx_type string,
       processor string,
       status string,
       random_charge_id int,
       request_data variant,
       request_data_billingAddress_email string,
       request_data_email string,
       request_data_customer_email string,
       request_data__customer_email string,
       request_data_profile_email string,
       init_response_data variant,
       init_response_data_customer_email string,
       notification_response_data variant,
       cc_identifier string,
       cc_expiry_identifier string,
       bin string,
       mid string,
       threeds variant,
       payment_method string,
       payment_instrument_type string,
       payment_model_id string,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.processors_mids (
       id int,
       processor_mid_id int,
       descriptor string,
       display_name string,
       config variant,
       processor_name string,
       fee number(6,5),
       partner_name string,
       is_default boolean,
       is_active boolean,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.quotes (
       quote_id string,
       broker_id int,
       wallet_id string,
       digital_currency string,
       fiat_currency string,
       digital_amount number(38,22),
       fiat_amount number(23,7),
       valid_until TIMESTAMP_TZ,
       broker_rate number(38,22),
       payment_id int,
       raw_response variant,
       created_at TIMESTAMP_TZ,
       payment_method string,
       rate_category string,
       blockchain_txn_fee number(38,22),
       fee_calculation_id string,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.r_payments (
           id int,
       simplex_payment_id int,
       api_payment_uuid string,
       partner_order_id string,
       inserted_at TIMESTAMP_TZ,
       created_at TIMESTAMP_NTZ,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);

CREATE TABLE public.refunds (
       id int,
       payment_id int,
       inserted_at TIMESTAMP_TZ,
       support_ticket_id int,
       simplex_end_user_id int,
       descriptive_reason string,
       user_id int,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.retrieval_request_types (
           reason_code string,
       retrieval_request_type string,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.retrieval_requests (
id int,
payment_id int,
inserted_at TIMESTAMP_TZ,
posted_at TIMESTAMP_TZ,
reason_code string,
user_id int,
raw_data variant,
retrieval_requests string,
raw_data_email_address string,
batch_id string,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.status_meaning (
           code int,
       description string,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE public.verification_requests (
           id int,
       simplex_end_user_id int,
       payment_id int,
       allow_verifications variant,
       verification_type string,
       inserted_at TIMESTAMP_TZ,
       status string,
       token string,
       requesting_user_id int,
       finalized_at TIMESTAMP_NTZ,
       verification_format string,
       parent_id  int,
       verifier_response variant,
       verifier_responded_at TIMESTAMP_NTZ,
       source string,
       source_wallet_poid_id int,
       resend_of int,
       verifier_name string,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);

CREATE TABLE public.verifications (
	ID NUMBER(38,0),
	SIMPLEX_END_USER_ID NUMBER(38,0),
	REQUESTING_USER_ID NUMBER(38,0),
	VERIFICATION_TYPE VARCHAR(16777216),
	FINALIZING_USER_ID NUMBER(38,0),
	STATUS VARCHAR(16777216),
	PARTNER_END_USER_ID NUMBER(38,0),
	INITIAL_PAYMENT_ID NUMBER(38,0),
	INSERTED_AT TIMESTAMP_TZ(9),
	FINALIZED_AT TIMESTAMP_NTZ(9),
	DW_CREATED_AT TIMESTAMP_NTZ(9) NOT NULL DEFAULT CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP_NTZ(9))
);
CREATE TABLE public.payment_status_log(id int,
payment_id int,
status int,
previous_status int,
created_at TIMESTAMP_NTZ,
details variant,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);

CREATE TABLE public.r_partner_decision_events(id int,
inserted_at TIMESTAMP_NTZ,
published_at TIMESTAMP_NTZ,
created_at TIMESTAMP_NTZ,
payment_id int,
partner_specific variant,
decision string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.r_payment_external_alerts(id int,
inserted_at TIMESTAMP_NTZ,
created_at TIMESTAMP_NTZ,
published_at TIMESTAMP_NTZ,
alert_source_service string,
data_raw string,
payment_id int, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.simplex_end_users(id int,
email string,
state string,
country string,
phone string,
created_at TIMESTAMP_NTZ,
updated_at TIMESTAMP_NTZ,
has_support_ticket boolean,
user_risk_status string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.r_payment_events(id int,
payment_id int,
event_type string,
peu_id int,
created_at TIMESTAMP_NTZ,
inserted_at TIMESTAMP_NTZ,
partner_session variant,
simplex_session variant,
amount_to_charge number(20,4),
currency_to_charge string,
country_raw string,
state_raw string,
cc_auth_id int,
card_token_id string,
email_raw string,
phone_raw string,
partner_seller_id int,
partner_specific variant,
partner_specific_payment_request_email string,
cc_identifier string,
cc_expiry_identifier string,
bin string,
amount_to_charge_usd number(20,4),
amount_usd_conversion_time TIMESTAMP_NTZ,
amount_usd_conversion_rate number(20,4),
routing_number string,
bank_identifier string,
payment_type string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.user_browser_events(id int,
event_type string,
request_data variant,
request_data_email string,
response_data variant,
api_requests variant,
inserted_at TIMESTAMP_NTZ,
payment_id int, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.payment_device_details(created_at TIMESTAMP_TZ,
payment_id int,
ip string,
uaid string,
user_agent_header string,
id int, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE account_processor.payment_attempts_account(id int,
created_at TIMESTAMP_TZ,
reference_id string,
ib_user_id string,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.payments_auth_requests(payment_id int,
auth_request_id string,
request_data variant,
created_at TIMESTAMP_NTZ,
payment_attempt_reference_id string,
updated_at TIMESTAMP_TZ,
payment_attempt_id string,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.enrich_ekata(id int,
request_data variant,
request_data_primary_email_address string,
data variant,
inserted_at TIMESTAMP_NTZ,
api_version string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.payment_attempts(id int,
payment_id int,
payment_method string,
reference_id string,
created_at TIMESTAMP_TZ,
payment_instrument_id string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.processors(id int,
name string,
is_default boolean
is_active boolean
routing variant,
mids variant,
acquirer_bins variant,
mpi_provider string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.email_verifications (id int,
partner_end_user_email_id int,
email_verification_id string,
status string,
created_at TIMESTAMP_TZ,
attempt int,
payment_id int,
otp_service_id string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.payment_local_currencies(payment_id int,
is_local_currency_chosen boolean,
is_user_choice boolean,
initial_currency string,
initial_amount number(20,4),
local_currency string,
local_amount number(20,4),
reason string,
cc_identifier string,
quote_id string,
created_at TIMESTAMP_TZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.payment_locales(payment_id int,
created_at TIMESTAMP_NTZ,
locale string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.verification_request_inquiries(id int,
verification_request_id int,
poid_inquiry_reference string,
created_at TIMESTAMP_TZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.verification_request_inquiries_checks(id int,
verification_request_inquiry_id int,
poid_check_reference string,
poid_check_decision_id string,
created_at TIMESTAMP_TZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.payments_auth_request_mpi_enrollments(mpi_enrollments_id int,
auth_request_id string,
created_at TIMESTAMP_NTZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.mpi_enrollments(id int,
created_at TIMESTAMP_NTZ,
mpi_provider string,
transaction_id string,
is_enrolled boolean,
enrollment_res string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.payments_auth_request_mpi_authentication_validations(mpi_authentication_validation_id int,
auth_request_id string,
created_at TIMESTAMP_NTZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.mpi_authentication_validation(id int,
created_at TIMESTAMP_NTZ,
mpi_provider string,
transaction_id string,
three_d_result string,
eci string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.payments_auth_request_proc_requests(proc_request_id int,
auth_request_id string,
created_at TIMESTAMP_NTZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.payment_auth_request_threeds(auth_request_id string,
threeds_id string,
created_at TIMESTAMP_TZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE threeds_svc.results(threeds_id string,
inserted_at TIMESTAMP_TZ,
version string,
threeds_provider string,
status string,
eci string,
authentication_value string,
ds_transaction_id string,
challenge_type string,
errors variant,
raw_response variant, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE threeds_svc.results(threeds_id string,
inserted_at TIMESTAMP_TZ,
version string,
threeds_provider string,
status string,
eci string,
authentication_value string,
ds_transaction_id string,
challenge_type string,
errors variant,
raw_response variant, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.fraud_warnings(id int,
inserted_at TIMESTAMP_NTZ,
import_id int,
payment_created_at TIMESTAMP_NTZ,
reported_at TIMESTAMP_NTZ,
payment_id int,
processor string,
card_scheme string,
masked_credit_card string,
fraud_type_raw string,
fraud_amount number(20,4),
fraud_currency string,
transaction_amount number(20,4),
transaction_currency string,
chargeback_occured boolean,
account_closed boolean,
raw_data variant,
raw_data_customer_email string,
raw_data_email_address string,
fraud_type_code string,
payment_captured_at TIMESTAMP_NTZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.chargeback_decisions(id int,
inserted_at TIMESTAMP_NTZ,
payment_id int,
fraud_type string,
decider_user_id int,
sub_fraud_type string,
fraud_type_conf_score int,
sub_fraud_type_conf_score int, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.chargeback_types(reason_code string,
chargeback_type string,
card_present boolean, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.chargebacks(id int,
payment_id int,
inserted_at TIMESTAMP_NTZ,
posted_at TIMESTAMP_NTZ,
reason_code string,
is_simplex_liable boolean,
simplex_end_user_id int,
user_id int,
raw_data variant,
raw_data_customer_email string,
raw_data_email_address string,
status string,
batch_id string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.fraud_warning_classifications(id int,
inserted_at TIMESTAMP_NTZ,
decider_user_id int,
payment_id int,
fraud_type string,
sub_fraud_type string,
fraud_type_conf_score int,
sub_fraud_type_conf_score int, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.r_chargebacks(id int,
inserted_at TIMESTAMP_NTZ,
published_at TIMESTAMP_NTZ,
payment_id int,
peu_id int,
type_raw string,
created_at TIMESTAMP_NTZ,
issuer_reason_code_raw string,
issuer_reason_description_raw string,
cc_identifier string,
cc_expiry_identifier string,
bin string,
cc_additional_data variant,
processor string,
dispute_status_raw string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.r_fraud_warnings(id int,
inserted_at TIMESTAMP_NTZ,
created_at TIMESTAMP_NTZ,
published_at TIMESTAMP_NTZ,
fraud_type_raw string,
processor string,
payment_id int,
cc_identifier string,
cc_expiry_identifier string,
bin string,
cc_additional_data variant, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.r_payments_analytic_tags(id int,
inserted_at TIMESTAMP_NTZ,
created_at TIMESTAMP_NTZ,
published_at TIMESTAMP_NTZ,
updated_by_dss_user_id int,
tags variant,
tags_change_log variant,
r_payment_id int, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.fraud_only_events_audit(id int,
payment_uuid string,
request variant,
request_user_email_details_email string,
request_seller_email string,
request_payment_request_email string,
response variant,
response_request_seller_email string,
response_request_payment_request_email string,
inserted_at TIMESTAMP_NTZ,
updated_at TIMESTAMP_NTZ,
partner_id int, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.phone_verifications(id int,
phone string,
payment_id int,
partner_end_user_id int,
verification_request_id string,
verified_at TIMESTAMP_NTZ,
inserted_at TIMESTAMP_NTZ,
api_request_data variant,
api_response_data variant,
is_current boolean, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.users(id int,
email string,
test_mode boolean
config variant, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.kyc_extra_data(id int,
simplex_end_user_id int,
partner_end_user_id int,
verification_request_id int,
inserted_at TIMESTAMP_NTZ,
user_data_state string,
user_data_issue_country string,
user_data_country_of_residence string,
same_as_billing_address boolean,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.fraud_warning_types(code int,
reason string,
description string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.uploads(id int,
filename string,
original_filename string,
inserted_at TIMESTAMP_NTZ,
simplex_end_user_id int,
http_session_id int,
verification_request_id int,
edited_at TIMESTAMP_NTZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE public.enrich_email_age(id int,
email string,
ip string,
inserted_at TIMESTAMP_NTZ,
data variant,
data_email string,
data_ipaddress string,
request_data_ip string,
request_data_email string,
version int, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);


create TABLE SIMPLEX_RAW.PUBLIC.AB_TEST_DECISIONS (
	ID NUMBER(38,0),
	PAYMENT_ID NUMBER(38,0),
	TEST_NAME VARCHAR(16777216),
	DECISION NUMBER(38,0),
	INSERTED_AT TIMESTAMP_NTZ(9),
	PAYMENT_UUID VARCHAR(16777216),
	PARTNER_END_USER_ID VARCHAR(16777216),
	PARTNER_ID NUMBER(38,0),
	UAID VARCHAR(16777216),
	DW_CREATED_AT TIMESTAMP_NTZ(9) NOT NULL DEFAULT CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP_NTZ(9)));

create table SIMPLEX_RAW.public.second_quote_revenue
(
    PAYMENT_ID                         NUMBER(38,0),
    latest_quote_price_breakdown_id    VARCHAR,
    execution_quote_price_breakdown_id VARCHAR,
    latest_quote_id                    VARCHAR,
    execution_quote_id                 VARCHAR,
        user_revenue                   VARIANT,
    tp_fee_revenue                     VARIANT,
    total_second_quote_revenue_usd     NUMBER(20, 4),
    CREATED_AT                         TIMESTAMP_NTZ,
    SYNTHETIC_KEY                      VARCHAR,
    DW_CREATED_AT                      TIMESTAMPNTZ default CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP_NTZ(9)) not null
);
