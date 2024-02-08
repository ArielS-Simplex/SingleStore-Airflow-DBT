CREATE TABLE account_processor.payment_attempts_account(
             id int,
             created_at TIMESTAMP_TZ,
             reference_id string,
             ib_user_id string,
             email string,
             dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);


