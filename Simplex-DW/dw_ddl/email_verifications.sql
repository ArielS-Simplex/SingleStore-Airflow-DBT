CREATE TABLE email_verifications.email_verifications(id int,
identifier string,
email_id int,
token string,
expires_in_minutes int,
created_at TIMESTAMP_NTZ,
redirect_url string, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);