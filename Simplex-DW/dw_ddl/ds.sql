CREATE TABLE ds.payment_label_index(
c1 int,
c2 string,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

CREATE TABLE ds.payoneer_ftp_chargebacks(partner_order_id int, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);

create or replace TABLE SIMPLEX_RAW.DS.frynomaly_results
(
    id                         integer,
    created_at                 TIMESTAMP_NTZ,
    preset_name                string,
    application_name           string,
    group_type                 string,
    group_name                 string,
    matrix_name                string,
    anomaly_date               TIMESTAMP_NTZ,
    significance_level         int,
    aggregation_time_interval  string,
    total_num_of_payments      integer,
    context_group_name         string,
    anomaly_features           variant,
    anomaly_payments_ids       string,
    anomaly_payments_ids_query string,
    dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);

CREATE TABLE SIMPLEX_RAW.DS.frynomaly_decisions
(
    id int,
    inserted_at TIMESTAMP_TZ,
    frynomaly_result_id int,
    decision string,
    reason string,
    user_id int,
    dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);

CREATE TABLE SIMPLEX_RAW.DS.mv_ds_labels
(
    simplex_payment_id          int,
    r_payment_id                int,
    time_point                  TIMESTAMP_NTZ,
    is_fraud_ring               boolean,
    fraud_type                  string,
    cb_fw_inserted_at           TIMESTAMP_NTZ,
    payment_label               string,
    real_fraud_post_auth        boolean,
    cb_or_fw_post_auth          boolean,
    friendly_fraud_post_auth    boolean,
    not_real_cb_or_fw_post_auth boolean,
    dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);

CREATE TABLE SIMPLEX_RAW.DS.mv_fraud_ring_tags
(
    email            string,
    payment_id       int,
    rental_scam      boolean,
    third_party_scam boolean,
    tag              string,
    inserted_at      TIMESTAMP_NTZ,
    dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
)