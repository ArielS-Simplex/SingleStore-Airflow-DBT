CREATE TABLE credit_cards.apple_pay_payment_attempts (
       id string,
       raw_payment_method_type string,
       raw_payment_method_network string,
       payment_instrument_id string,
       normalized_network_id int,
       payment_attempt_raw_id string,
       created_at TIMESTAMP_TZ,
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE credit_cards.basic_card_credit_card (
id int,
card_id int,
credit_card_base_id int,
created_at TIMESTAMP_TZ,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE credit_cards.credit_cards (
id int,
credit_card_base_id int,
state string,
country string,
created_at TIMESTAMP_TZ,
wallet_type string,
data variant,
data_email_address string,
data_email string,
data_raw_data_email string,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE credit_cards.payment_attempts_raw (
     id string,
     response variant,
     response_email string,
     response_email_address string,
     created_at TIMESTAMP_TZ,
     dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE credit_cards.payment_instruments (
id string,
state string,
country string,
email string,
phone string,
type string,
created_at TIMESTAMP_TZ,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
card_network variant,
);

CREATE TABLE credit_cards.credit_cards_base(id int,
masked_credit_card string,
expiry_month int,
expiry_year int,
created_at TIMESTAMP_NTZ,
normalized_network_id int, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);