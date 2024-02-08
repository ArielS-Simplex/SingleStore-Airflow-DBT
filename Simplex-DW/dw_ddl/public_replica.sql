CREATE TABLE public_replica.fee_strategies(name string,
model string,
configuration variant,
created_at TIMESTAMP_TZ, dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ);