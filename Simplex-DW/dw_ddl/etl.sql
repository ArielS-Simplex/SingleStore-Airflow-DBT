CREATE TABLE etl.bitstamp_prices (
           id int,
     currency_pair string,
     ts TIMESTAMP_TZ,
     price number(20,4),
     volume number(20,10),
     dw_created_at timestamp_ntz not null default CURRENT_TIMESTAMP(0)::TIMESTAMP_NTZ
);