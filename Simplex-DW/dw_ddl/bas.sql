CREATE TABLE bas.ib_registration(id string,
class string,
data variant,
data_implexPrefillPayload_email string,
data_email string,
data_nitiationRequest_browserUserAgents string,
lastsrequest_requesttime TIMESTAMP_TZ,
dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);