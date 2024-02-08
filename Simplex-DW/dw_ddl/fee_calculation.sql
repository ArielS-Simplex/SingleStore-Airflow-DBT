CREATE TABLE fee_calculation.applied_rules (
           created_at TIMESTAMP_TZ,
       id int,
       calculation_id string,
       name string,
       effective_params variant,
       lp_service_fee_percent number(4,2),
       dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);
CREATE TABLE fee_calculation.calculations_result (
      id string,
      created_at timestamp_tz,
      lp_service_fee_percent number(4,2),
      dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);