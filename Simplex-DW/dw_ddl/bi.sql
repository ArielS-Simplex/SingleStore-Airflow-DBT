CREATE TABLE bi.onfido_client_events(id int,
created_at TIMESTAMP_TZ,
onfido_event_name string,
nano_ib_user_id string,
nano_ib_user_party_id string,
nano_token string,
user_agent string,
url string,
onfido_raw_data variant,
platform string,
reference_id string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE bi.onfido_results(id string,
applicant_id string,
created_at TIMESTAMP_TZ,
email string,
document_result string,
document_status string,
photo_result string,
photo_status string,
video_result string,
video_status string,
document_type string,
document_issuing_country string,
raw_document_report variant,
raw_photo_report variant,
raw_video_report variant,
document_decision_reason variant,
platform string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);



CREATE TABLE bi.onfido_results(id string,
applicant_id string,
created_at TIMESTAMP_TZ,
email string,
document_result string,
document_status string,
photo_result string,
photo_status string,
video_result string,
video_status string,
document_type string,
document_issuing_country string,
raw_document_report variant,
raw_photo_report variant,
raw_video_report variant,
document_decision_reason variant,
platform string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);


create table bi.processing_cost_share(
 bb_card_brand          string,
    processing_cost_reigon string,
    final_card_type        string,
    processor              string,
    forqlik                string,
    processing_cost_share  NUMBER(20, 18),
    DW_CREATED_AT          TIMESTAMPNTZ default CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP_NTZ(9)) not null
)
