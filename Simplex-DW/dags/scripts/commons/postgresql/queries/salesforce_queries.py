# get chargeback data from salesforce
CHARGEBACK_QUERY = """with Upload_to_sf as (
select distinct case
   when ct.chargeback_type = 'fraud' and ct.reason_code in ('11.3', '4808') then 'Turnkey Service Level'
   when ct.chargeback_type = 'fraud' and ct.reason_code not in ('11.3', '4808')  then 'Fraud'
   when ct.chargeback_type = 'service_level' --and p.pay_to_partner_id in (38, 52, 67, 176, 278, 291,321, 350, 419, 585)
   then 'Turnkey Service Level'--Bitstamp Broker,K1-broker,Paybis-broker,Elastum-broker, Paybis US, Nevada
   --when ct.chargeback_type = 'service_level' and p.pay_to_partner_id in (23, 50)
   --then 'Paybis - Service Level'--PayBis,PayBis LTD
   --when ct.chargeback_type = 'service_level' and p.pay_to_partner_id in (178)
   --then 'Elastum - Service Level'--Elastum
   --when ct.chargeback_type = 'service_level' and p.pay_to_partner_id not in (38, 52, 67, 176, 23, 50, 178, 48, 278, 291, 321, 350, 419, 585)
   --then 'Others - Service Level'
   else 'Error' end as CB_Template,
   case
   when process.name = 'ecp' and p.bin like '4%'
   then date(c.posted_at + interval '8 days')--For ECP's Visa CBs
   when process.name = 'ecp' and p.bin like '5%'
   then date(c.posted_at + interval '8 days')--For ECP's Mastercard CBs
   when process.name = 'ecp' and p.bin like '6%'
   then date(c.posted_at + interval '8 days')--For ECP's Mastercard CBs
   when process.name = 'paysafe' then date(c.posted_at + interval '7 days')--Paysafe said 14, but having issues with it
   --when process.name = 'worldline' then date(c.posted_at + interval '7 days')--For Worldline's CBs
   when process.name = 'safecharge' and p.pay_to_partner_id not in (419) then date(c.posted_at + interval '5 days')--SC gives us 14, but recommend 7
   when process.name = 'safecharge' and p.pay_to_partner_id in (419) then date(c.posted_at + interval '5 days')--TYTSgives 10 days
   end as Fight_Until,--no else condition
   replace(coalesce((c.raw_data #>> '{arn}'), (c.raw_data #>> '{ARN}'),
   (c.raw_data #>> '{Acquirer ref number}'),
   (c.raw_data #>> '{Acquirer Ref Number}')), '''', '') as ARN,--ecp,paysafe,worldline
   p.id as Simplex_ID,
   p.cc_identifier as Credit_Card_Identifier,
   coalesce(cd.fraud_type, fwc.fraud_type) as Analysts_Decision,
   coalesce(cd.sub_fraud_type, fwc.sub_fraud_type) as Sub_Classification,
   --case when r.raw_data is not null then 'Yes' else 'No' end as Was_RRQ,
   case
   when p.pay_to_partner_id in (1, 15, 41, 54, 90, 118, 161) then 'Top-Up'--Since for Genesis,BTCC,Xapo,EXMO,Latoken,Quine,OKCoin we have BTC on every payment
   else p.crypto_currency end as Crypto_Curr,
   par.full_name as Partner_Name,
   p.phone as Phone,
   par.id as Account_ID,--for SF purposes
   case
   when ((pr.threeds->>'eci' IN ('1','2','5','6', '01', '02', '05', '06')) and (pr.threeds#>>'{cavv}' is not null)) and par.full_name <> 'Paybis-US-broker'
       then 'Yes'
   when pr.threeds#>>'{cavv}' is not null and (pr.raw_response -> 'paymentOption' -> 'card' -> 'threeD' ->> 'eci' IN ('1','2','5','6', '01', '02', '05', '06')) and par.full_name = 'Paybis-US-broker'
       then 'Yes'
   else 'No' end as threeds,
   case when fw.reported_at is not null then 'Yes' else 'No' end as Has_FW,
   case when p.status in (13, 15) then 'Yes' else 'No' end as was_refunded,--Was Refunded
   ct.chargeback_type as CB_Type,
   c.reason_code,
   --coalesce((c.raw_data #>> '{reason_description}'),--ECP RCs
   --(c.raw_data #>> '{Reason Description}'),--Paysafe RCs
   case when c.reason_code = '10.5' then 'Visa Fraud Monitoring Program'
        when c.reason_code = '4870 ' then 'Chip Liability Shift'
        when c.reason_code = '4871' then 'Chip/PIN Liability Shift'
   else ct.reason_description end as reason_description,
   --ct.reason_description,--ecp,paysafe,worldline don't have it
   cast(p.total_amount as decimal(10, 2)) as Pay_Amt,
   p.currency as Pay_Curr,
   case
   when process.name = 'ecp' then text(
   cast(((c.raw_data #>> '{amount}')::decimal) / -100 as decimal(10, 2)))
   when process.name = 'paysafe' then
   replace(coalesce((c.raw_data#>>'{CB Posting Amount}'),(c.raw_data#>>'{Posting Amount}')),',','')
   --when process.name = 'worldline' and c.raw_data #>>'{TRX Currency Code}'='392'--only JPY isn't divided by 100
    --then text(cast(((c.raw_data #>> '{TRX Amount}')::decimal) as decimal(10, 2)))
   --when process.name = 'worldline' and c.raw_data #>>'{TRX Currency Code}'!='392'-- all other currencies are divided by 100
    --then text(cast(((c.raw_data #>> '{TRX Amount}')::decimal) / 100 as decimal(10, 2)))
   when process.name = 'safecharge' then c.raw_data #>> '{chargebackAmount}'
   end as CB_Amt,
   case
   when process.name = 'ecp' then 'USD'
   when process.name = 'paysafe' then c.raw_data #>> '{Original Transaction Currency}'
   /*when process.name = 'worldline' then (
    case
    when c.raw_data #>> '{TRX Currency Code}' = '36' then 'AUD'
    when c.raw_data #>> '{TRX Currency Code}' = '124' then 'CAD'
    when c.raw_data #>> '{TRX Currency Code}' = '203' then 'CZK'
    when c.raw_data #>> '{TRX Currency Code}' = '208' then 'DKK'
    when c.raw_data #>> '{TRX Currency Code}' = '348' then 'HUF'
    when c.raw_data #>> '{TRX Currency Code}' = '392' then 'JPY'
    when c.raw_data #>> '{TRX Currency Code}' = '554' then 'NZD'
    when c.raw_data #>> '{TRX Currency Code}' = '578' then 'NOK'
    when c.raw_data #>> '{TRX Currency Code}' = '643' then 'RUB'
    when c.raw_data #>> '{TRX Currency Code}' = '710' then 'ZAR'
    when c.raw_data #>> '{TRX Currency Code}' = '752' then 'SEK'
    when c.raw_data #>> '{TRX Currency Code}' = '756' then 'CHF'
    when c.raw_data #>> '{TRX Currency Code}' = '826' then 'GBP'
    when c.raw_data #>> '{TRX Currency Code}' = '840' then 'USD'
    when c.raw_data #>> '{TRX Currency Code}' = '949' then 'TRY'
    when c.raw_data #>> '{TRX Currency Code}' = '985' then 'PLN'
    when c.raw_data #>> '{TRX Currency Code}' = '977' then 'BAM'
    when c.raw_data #>> '{TRX Currency Code}' = '978' then 'EUR'
    when c.raw_data #>> '{TRX Currency Code}' = '404' then 'KES'
    else c.raw_data #>> '{TRX Currency Code}' end)*/
   when process.name = 'safecharge' then c.raw_data #>> '{currency}' end as CB_Amt_Curr,
   cast(p.total_amount_usd as decimal(10, 2)) as Pay_Amt_USD,
   p.handling_at as Transaction_Date,
   date(c.posted_at) as CB_Posting_Date,
   date(c.posted_at + interval '135 day') as Final_Decision_Date,
   'New' as Status,
   c.status as Stage,
   'TBD' as Decision,
   process.name as Processor,
   --case when is_simplex_liable = 'TRUE' then 'Yes' else 'No' end as Is_Simplex_Liable,
   case
   when ((ct.chargeback_type='service_level' and p.pay_to_partner_id in (38, 52, 67, 176, 278, 291, 321, 419, 585)) or
   p.pay_to_partner_id in (419)) and ppp.id is not null then 'Yes'
   when ct.chargeback_type='fraud' and pr.threeds#>>'{cavv}' is null and ppp.id is not null then 'Yes'
   else 'No' end as Is_Simplex_Liable,
   --case when ct.card_present = 'TRUE' then 'Yes' else 'No' end as Card_Present,
   case
   when p.bin like '4%' then 'Visa'
   when p.bin like '2%' then 'Mastercard'
   when p.bin like '5%' then 'Mastercard'
   when p.bin like '6%' then 'Mastercard'
   when p.bin is null and pr.payment_instrument_type = 'google_pay' and c.raw_data#>>'{paymentMethod}' like 'Visa'  then 'GooglePay-Visa'
   when p.bin is null and pr.payment_instrument_type = 'google_pay' and c.raw_data#>>'{paymentMethod}' like 'Master%' then 'GooglePay-Mastercard'
   when p.bin is null and c.raw_data#>>'{paymentMethod}' like 'Visa' then 'ApplePay-Visa'
   when p.bin is null and c.raw_data#>>'{paymentMethod}' like 'Master%' then 'ApplePay-Mastercard'
   else 'Error' end as Card_Scheme,
   case
    when em.data -> 'credit_card' -> 'issuer' ->> 'name' is not null then em.data -> 'credit_card' -> 'issuer' ->> 'name'
    when p.bin is null and eb.issuing_organization is null and concat(c.raw_data #>>'{issuer_bank}', c.raw_data #>>'{issuerBank}') not in ('') then concat(c.raw_data #>>'{issuer_bank}', c.raw_data #>>'{issuerBank}') --take issuer bank from chargeback for Apple Pay
    when p.bin is null and eb.issuing_organization is null and concat(c.raw_data #>>'{issuer_bank}', c.raw_data #>>'{issuerBank}')='' then 'Unknown' --AP
    when p.bin is null and eb.issuing_organization is null and concat(c.raw_data #>>'{issuer_bank}', c.raw_data #>>'{issuerBank}') ='' and pr.payment_instrument_type = 'google_pay' then 'Unknown' --GP
    when p.bin is not null and eb.issuing_organization is not null then eb.issuing_organization
    else 'Unknown' end as Issuing_Bank,
   case when eb.issuing_country_iso_a2 is not null then eb.issuing_country_iso_a2
   else p.country end as BIN_Country,
   p.order_id as Partner_ID,
   p.payment_id as Simplex_Long_ID,
   cast(sum_cc.SUMC as decimal(10, 2)) as Total_Amt_by_Card,
   coalesce((c.raw_data#>>'{original_transaction_unique_id}'),--ECP
    (c.raw_data #>> '{Transaction ID}'),--Paysafe
    --(c.raw_data #>> '{CB Case ID}'),--Worldline old version
    --(c.raw_data #>> '{Case Id}'),--Worldline current version
    (c.raw_data #>> '{transactionId}')--SafeCharge
    ) as UTI,
   coalesce((pm.config -> 'accounts' -> p.currency ->> 'account'),
   (pm.config -> 'merchant' ->> p.currency),
   (pm.config #>> '{merchant,uId}'),
   (pm.config #>> '{merchantId}')) as MID, --need to be checked for SC
   pm.descriptor as Descriptor,
   case
   when ppp.full_name is null then pm.descriptor
   else ppp.full_name end as "turnkey/exchange",
   ppp.id as "TK ID",
   p.crypto_currency_address as crypto_address,
   qq.digital_amount as crypto_amount,
   replace(replace(replace(concat(first_name_card,' ',p.last_name_card),',',''),'.',''),'#','')
   as full_name,
   p.email,
   replace(replace(replace(concat(p.address1,' ',p.city,' ',p.state,' ',p.country,' ',p.zipcode),',',''),'.',''),'#','')
   as billing_address,
   case
   when process.name = 'ecp' and p.bin like '4%'
   then date(c.posted_at + interval '21 days')--For ECP's Visa CBs
   when process.name = 'ecp' and p.bin like '5%'
   then date(c.posted_at + interval '30 days')--For ECP's Mastercard CBs
   when process.name = 'paysafe' then date(c.posted_at + interval '15 days')
   --when process.name = 'worldline' then date(c.posted_at + interval '20 days')
   when process.name = 'safecharge' and p.pay_to_partner_id not in (419) then date(c.posted_at + interval '14 days')
   when process.name = 'safecharge' and p.pay_to_partner_id in (419) then date(c.posted_at + interval '10 days')
   end as AB_Fight_Until,
   case when u.event_type='confirm-coin-delivery'
    then 'Yes'
    else 'No'
   end as email_confirmation
   from payments as p
   left join chargebacks as c on c.payment_id = p.id
   left join chargeback_types as ct on ct.reason_code = c.reason_code-- added another line
   left join (select distinct eb.bin, eb.issuing_organization, eb.issuing_country_iso_a2
   from enrichments.binbase_new as eb
   where eb.issuing_organization notnull
   and eb.ver in (14)) as eb on p.bin = eb.bin
   -- enrichments.binbase_new eb on p.bin = eb.bin
   left join chargeback_decisions as cd on cd.payment_id = p.id
   left join quotes as qq on p.quote_id = qq.quote_id
   left join partners ppp on qq.wallet_id = ppp.name
   left join fraud_warning_classifications as fwc on fwc.payment_id = p.id
   left join retrieval_requests as r on r.payment_id = p.id--there's a left join so the null will also appear
   left join partners as par on par.id = p.pay_to_partner_id
   left join proc_requests as pr on pr.payment_id = p.id
   left join fraud_warnings fw on fw.payment_id = p.id--there's a left join so the null will also appear
   left join processors as process on p.processor_id = process.id
   left join enrich_maxmind as em
   on em.inserted_at::date = p.created_at::date and em.request_data ->> 'bin' = p.bin and
   em.request_data ->> 'i' = p.simplex_login ->> 'ip'
   left join public_replica.processors_mids as pm on p.mid = pm.descriptor
   left join payments_auth_requests as parq
   on parq.payment_id = p.id and
   (parq.request_data #>> '{processorId}')::int = p.processor_id
   left join (--group by the credit card and the posted_at_date
   select mid_agg.cc_identifier, mid_agg.posted_at as posted_at_date, sum(mid_agg.total_amount_usd_cc) as SUMC
   from (
    select distinct cc.payment_id, pp.cc_identifier, date(cc.posted_at) as posted_at_date, pp.total_amount_usd
    from payments as pp join chargebacks as cc on pp.id = cc.payment_id
    where pp.status in (2, 13, 15, 9)
    and cc.status = '1st_chargeback') as mid_agg(payment_id, cc_identifier,posted_at,total_amount_usd_cc)
   group by cc_identifier, posted_at) as sum_cc
   on (sum_cc.cc_identifier = p.cc_identifier and sum_cc.posted_at_date = date(c.posted_at))
   left join (select pel1.payment_id, pel1.partner_id, name from payment_events_log pel1) pel on pel.payment_id=p.id
   left join user_browser_events u on u.payment_id=c.payment_id
   where c.status = '1st_chargeback'--the other stages will be updated manually
   and p.status in (2, 13, 15, 9)--only for approved payments
   and pr.tx_type = 'authorization'
   and pr.status = 'success'
   and pel.name in('payment_simplexcc_execution','payment_simplexcc_approved','payment_simplexcc_execute')--execution for TKs, approved for direct
   and pel.partner_id in(23,67,111,176,178,291,321,350,419,585)--Exchanges and LPs
   and c.inserted_at > current_date - 5
    ----and c.posted_at>='2021-05-20'
   --and p.id in(32232691,32593222,22428280)
   --and p.processor_id=7
   and c.reason_code not in('2700')
   --and (c.raw_data#>>'{status}' in('Regular') or c.raw_data#>>'{status}' is null or c.raw_data#>>'{chargebackStatus}'
   --in('Regular')
   --)
   order by Fight_Until, Total_Amt_by_Card desc, Pay_Amt_USD desc, CB_Posting_Date, Transaction_Date
), Fraud_Query as ( with cb_payments as (
       select p.id as payment_id, cb.posted_at, p.cc_identifier, p.cc_expiry_identifier, p.status, p.simplex_end_user_id, p.crypto_currency_address, concat(first_name_card,' ',last_name_card) as full_name,first_name_card,last_name_card, concat(seu.first_name,' ', seu.last_name) as full_acc_name, seu.first_name as first_name, seu.last_name as last_name,
       coalesce((cb.raw_data #>> '{arn}'), (cb.raw_data #>> '{ARN}'),
           (cb.raw_data #>> '{Acquirer ref number}'),
           (cb.raw_data #>> '{Acquirer Ref Number}')) as ARN, pr.raw_response#>> '{last4Digits}' as Last4_CC
   from chargebacks as cb
       join payments as p on cb.payment_id = p.id left join proc_requests pr on p.id = pr.payment_id
       join simplex_end_users as seu on p.simplex_end_user_id = seu.id
   where
       cb.inserted_at > current_date - 5  --it was set to 30days before
       and cb.status = '1st_chargeback' and
       (cb.reason_code = '10.4'or cb.reason_code = '4837' or cb.reason_code = '4849' or cb.reason_code = '4808' or cb.reason_code = '11.3' or cb.reason_code = '10.5' or cb.reason_code = '4840')
/*), card_chargebacks as (
   select cbp.payment_id, count(*) as card_num_chargebacks
   from cb_payments as cbp
       join payments as p on cbp.cc_identifier = p.cc_identifier
                       and cbp.cc_expiry_identifier = p.cc_expiry_identifier
       join chargebacks as cb on p.id = cb.payment_id
                       and cb.posted_at < cbp.posted_at
   where cb.inserted_at > current_date - 5
   group by 1*/
), avs as (
   select distinct on (cbp.payment_id)
       cbp.payment_id,
       case
           when pr.processor = 'ecp' and pr.raw_response ->> 'avs_response_code' in ('5A', '5B', '5D', '5F', '5M', '5P', '5W', '5X', '5Y', '5Z') then true
           when pr.processor = 'paysafe' and pr.raw_response ->> 'avsResponse' in ('MATCH', 'MATCH_ADDRESS_ONLY', 'MATCH_ZIP_ONLY') then true
           when pr.processor = 'safecharge' and pr.raw_response #>> '{paymentOption, card, avsCode}' in ('A', 'B', 'D', 'F', 'M', 'P', 'W', 'X', 'Y', 'Z') then true
           --when pr.processor = 'worldline' and pr.raw_response #>> '{POS-GWLink, AuthorisationResponse, Transaction, HostResponse, AVSResponseCode, _text}' in ('00', '02') then true
           else false
       end as avs_match
   from cb_payments as cbp
       join proc_requests as pr on cbp.payment_id = pr.payment_id
   where
       pr.tx_type = 'authorization'
   order by 1 asc, pr.created_at desc
), dvars as (
   select distinct on (d.payment_id)
       d.payment_id,
       buyer_mm_ip_ad_match,
       buyer_old_email_with_name_match,
       days_since_first_approved_payment,
       coalesce(variables#>>'{Analytic,variables,Analytic,name_on_payment_method_instrument_to_name_in_poid_valid_for_current_payment_match}',
           variables#>>'{Analytic,variables,Analytic,name_on_card_to_name_in_poid_valid_for_current_payment_match}')
           as name_on_instr_to_name_in_valid,
       --name_on_payment_method_instrument_to_name_in_poid_valid_for_current_payment_match
       user_has_approved_kyc_from_broker,
       user_verified_by_selfie,
       --buyer_device_type,
       --buyer_device_brand,
       --buyer_device_family,
       --buyer_device_model,
       buyer_ekata_address_phone_match,
       buyer_ekata_name_phone_match,
       buyer_email_name_match_level,
       buyer_ekata_email_name_match,
       buyer_num_approved_payments,
       buyer_num_chargebacks,
       --buyer_mm_device_id,
       buyer_phone_name_to_card_name_match_ea_ekata,
       was_auth_done_with_threeds,
       num_new_btc_address_users
   from
       cb_payments as cbp
       join decisions as d on cbp.payment_id = d.payment_id
       cross join lateral jsonb_to_record(d.variables #> '{Analytic, variables, Analytic}') as d1 (
           buyer_mm_ip_ad_match text,
           buyer_old_email_with_name_match text,
           days_since_first_approved_payment text,
           --name_on_payment_method_instrument_to_name_in_poid_valid_for_current_payment_match text,
           user_has_approved_kyc_from_broker bool,
           user_verified_by_selfie bool,
           --buyer_device_type text,
           --buyer_device_brand text,
           --buyer_device_family text,
           --buyer_device_model text,
           buyer_email_name_match_level text,
           buyer_ekata_email_name_match text,
           buyer_ekata_address_phone_match text,
           buyer_ekata_name_phone_match text,
           buyer_num_chargebacks numeric,
           buyer_num_approved_payments numeric,
           --buyer_mm_device_id text,
           buyer_phone_name_to_card_name_match_ea_ekata text,
           was_auth_done_with_threeds bool,
           num_new_btc_address_users numeric
       )
       left join chargebacks as cb on d.payment_id = cb.payment_id
   where d.application_name = 'Bender_Auto_Decide'
   order by d.payment_id, d.created_at desc
), payment_tags as (
   select distinct on (cbp.payment_id) cbp.payment_id, rpat.tags
   from
       r_payments_analytic_tags as rpat
       join r_payments as rp on rpat.r_payment_id = rp.id
       join cb_payments as cbp on cbp.payment_id = rp.simplex_payment_id
   order by cbp.payment_id asc, rpat.inserted_at desc
), fraud_rings as (
   select pt.payment_id, true as is_in_fraud_ring
   from
       payment_tags as pt
       cross join lateral jsonb_to_recordset(pt.tags) as b(name text)
   where
       b.name ilike '%fraud ring%'
), cb_transaction_days as (
   select distinct on (cbp.payment_id)
       cbp.payment_id,
       extract('days' from (cbp.posted_at - pr.updated_at)) as days_txn_cb
   from cb_payments as cbp
       join proc_requests pr on cbp.payment_id = pr.payment_id
   where
       pr.tx_type = 'capture-authorization'
       and pr.status = 'success'
   order by cbp.payment_id, pr.updated_at desc
), others as (
   select distinct on (p.id)
       p.id as payment_id,
       (em.data #>> '{billing_address, distance_to_ip_location}')::numeric as distance_to_ip_location,
       (em.data #>> '{ip_address, location, accuracy_radius}')::numeric as accuracy_radius,
       (em.data #>> '{ip_address, risk}')::numeric as risk,
       date_part('day', p.created_at - seu.created_at) as days_between_payment_and_account_creation,
       --#11
       -- cb posting date vs processing date return 666
       -- Check if the gap between the CB processing date and the CB posting date is less than 120 days – if the gap is bigger the 120 days – CB is invalid (Exception – the query should break and return 100)
       --#13
       case
           when not fw.id is null then true
           else false
       end as has_fraud_warning,
       date_part('day', cbp.posted_at - pr.updated_at) as days_between_txn_cb,
       par.full_name as partner
   from
       cb_payments as cbp
       left join payments as p on p.id = cbp.payment_id
       left join simplex_end_users as seu on p.simplex_end_user_id = seu.id
       left join r_payments as rp on p.id = rp.simplex_payment_id
       left join maxmind_triggers as mt on mt.r_payment_id = rp.id
       left join enrich_maxmind as em on em.id = mt.enrich_maxmind_id
       left join fraud_warnings as fw on cbp.payment_id = fw.payment_id
       left join chargebacks as cb on p.id = cb.payment_id
       left join proc_requests pr on cbp.payment_id = pr.payment_id
       left join partners par on par.id = p.pay_to_partner_id
), Undisputed_TRX as (
    Select distinct p2.*, coalesce((raw_data #>> '{arn}'),
             (raw_data #>> '{ARN}'),
             (raw_data #>> '{Acquirer ref number}'),
             (raw_data #>> '{Acquirer Ref Number}')) as num_of_chargebacks
from payments as p2 left join chargebacks as cbp on cbp.payment_id = p2.id join cb_payments on cb_payments.cc_identifier = p2.cc_identifier
where p2.status in (2) and p2.simplex_end_user_id in (cb_payments.simplex_end_user_id) and p2.cc_identifier is not null
),  Count_Undisputed as (
    SELECT cc_identifier, count(distinct id) as num_of_payments, count(distinct num_of_chargebacks) as num_of_chargebacks
    from Undisputed_TRX
    where cc_identifier = Undisputed_TRX.cc_identifier
    group by cc_identifier
) ,
    Undisputed_TRX_apple_pay as (
    Select distinct on (p2.id) p2.id, p2.simplex_end_user_id, pr.payment_instrument_type,pr.raw_response#>> '{last4Digits}' as cc_identifier,
                    coalesce((raw_data #>> '{arn}'),
                             (raw_data #>> '{ARN}'),
                             (raw_data #>> '{Acquirer ref number}'),
                             (raw_data #>> '{Acquirer Ref Number}')) as num_of_chargebacks
    from payments as p2
             left join chargebacks as cbp on cbp.payment_id = p2.id
             left join proc_requests pr on cbp.payment_id = pr.payment_id join cb_payments on cb_payments.Last4_CC = pr.raw_response#>> '{last4Digits}'
             where (pr.raw_response#>> '{last4Digits}', p2.id, p2.simplex_end_user_id)
             in (select distinct pr.raw_response#>> '{last4Digits}', pr.payment_id, payments.simplex_end_user_id
             from payments join proc_requests pr on payments.id = pr.payment_id
             where pr.tx_type = 'capture-authorization' and pr.payment_instrument_type = 'google_pay')
      and p2.status in (2)
      and p2.cc_identifier is null
),  Count_Undisputed_apple_pay as (
    SELECT  Undisputed_TRX_apple_pay.cc_identifier as cc_identifier, count(distinct Undisputed_TRX_apple_pay.id) as num_of_payments, count(distinct num_of_chargebacks) as num_of_chargebacks
    from Undisputed_TRX_apple_pay
    group by Undisputed_TRX_apple_pay.cc_identifier
),
    weights as (
   select distinct on (cbp.payment_id)
       cbp.payment_id, Count_Undisputed.num_of_payments as Number_of_payments_not_apple_pay, Count_Undisputed.num_of_chargebacks as num_of_chargebacks_not_apple_pay, Count_Undisputed_apple_pay.num_of_payments as Number_of_payments_apple_pay, Count_Undisputed_apple_pay.num_of_chargebacks as num_of_chargebacks_apple_pay,

       --#1
       case
           when cbp.ARN like '7%' and distance_to_ip_location < 100 and accuracy_radius < 25 and risk < 5 then 0.1 -- Visa all <
           when cbp.ARN like '7%' and distance_to_ip_location < 100 and accuracy_radius > 25 and risk > 5 then 0.1 -- Visa just bill add <
           when cbp.ARN like '7%' and distance_to_ip_location < 100 and accuracy_radius < 25 and risk > 5 then 0.1 -- Visa bill and acc
           when cbp.ARN like '7%' and distance_to_ip_location < 100 and accuracy_radius > 25 and risk < 5 then 0.1 -- Visa bill and proxy <
           when cbp.ARN like '7%' and distance_to_ip_location > 100 and accuracy_radius < 25 and risk < 5 then 0.1 -- Visa acc and proxy <
           when cbp.ARN like '2%' and distance_to_ip_location < 100 and accuracy_radius < 25 and risk < 5 then 0.1 -- Visa all <
           when cbp.ARN like '2%' and distance_to_ip_location < 100 and accuracy_radius > 25 and risk > 5 then 0.1 -- Visa just bill add <
           when cbp.ARN like '2%' and distance_to_ip_location < 100 and accuracy_radius < 25 and risk > 5 then 0.1 -- Visa bill and acc
           when cbp.ARN like '2%' and distance_to_ip_location < 100 and accuracy_radius > 25 and risk < 5 then 0.1 -- Visa bill and proxy <
           when cbp.ARN like '2%' and distance_to_ip_location > 100 and accuracy_radius < 25 and risk < 5 then 0.1 -- Visa acc and proxy <
           when cbp.ARN like '8%' and distance_to_ip_location < 100 and accuracy_radius < 25 and risk < 5 then 0.1 -- Mastercard all <
           when cbp.ARN like '8%' and distance_to_ip_location < 100 and accuracy_radius > 25 and risk > 5 then 0.1 -- Mastercard just bill add <
           when cbp.ARN like '8%' and distance_to_ip_location < 100 and accuracy_radius < 25 and risk > 5 then 0.1 -- Mastercard bill and acc
           when cbp.ARN like '8%' and distance_to_ip_location < 100 and accuracy_radius > 25 and risk < 5 then 0.1 -- Mastercard bill and proxy <
           when cbp.ARN like '8%' and distance_to_ip_location > 100 and accuracy_radius < 25 and risk < 5 then 0.1 -- Mastercard acc and proxy <
           when cbp.ARN like '0%' and distance_to_ip_location < 100 and accuracy_radius < 25 and risk < 5 then 0.1 -- Mastercard all <
           when cbp.ARN like '0%' and distance_to_ip_location < 100 and accuracy_radius > 25 and risk > 5 then 0.1 -- Mastercard just bill add <
           when cbp.ARN like '0%' and distance_to_ip_location < 100 and accuracy_radius < 25 and risk > 5 then 0.1 -- Mastercard bill and acc
           when cbp.ARN like '0%' and distance_to_ip_location < 100 and accuracy_radius > 25 and risk < 5 then 0.1 -- Mastercard bill and proxy <
           when cbp.ARN like '0%' and distance_to_ip_location > 100 and accuracy_radius < 25 and risk < 5 then 0.1 -- Mastercard acc and proxy <
           else 0

       end as weight_1,
       --#2
       case
           when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and (Count_Undisputed.num_of_payments > Count_Undisputed.num_of_chargebacks or Count_Undisputed_apple_pay.num_of_payments > Count_Undisputed_apple_pay.num_of_chargebacks) then 0.1 -- Mastercard Tango
           when cbp.ARN like '7%' and (Count_Undisputed.num_of_payments > Count_Undisputed.num_of_chargebacks or Count_Undisputed_apple_pay.num_of_payments > Count_Undisputed_apple_pay.num_of_chargebacks) then 0.25 -- Visa
           when cbp.ARN like '2%' and (Count_Undisputed.num_of_payments > Count_Undisputed.num_of_chargebacks or Count_Undisputed_apple_pay.num_of_payments > Count_Undisputed_apple_pay.num_of_chargebacks) then 0.25 -- Visa
           when cbp.ARN like '2%' and (Count_Undisputed.num_of_payments > Count_Undisputed.num_of_chargebacks or Count_Undisputed_apple_pay.num_of_payments > Count_Undisputed_apple_pay.num_of_chargebacks) then 0.25 -- Visa
           when cbp.ARN like '8%' and (Count_Undisputed.num_of_payments > Count_Undisputed.num_of_chargebacks or Count_Undisputed_apple_pay.num_of_payments > Count_Undisputed_apple_pay.num_of_chargebacks) then 0.1 -- Mastercard
           when cbp.ARN like '0%' and (Count_Undisputed.num_of_payments > Count_Undisputed.num_of_chargebacks or Count_Undisputed_apple_pay.num_of_payments > Count_Undisputed_apple_pay.num_of_chargebacks) then 0.1 -- Mastercard
           --when cbp.ARN like '9%' and (Count_Undisputed.num_of_payments > Count_Undisputed.num_of_chargebacks or Count_Undisputed_apple_pay.num_of_payments > Count_Undisputed_apple_pay.num_of_chargebacks) then 0.1 -- Mastercard
           else 0
       end as weight_2,
       --#3
       case
           when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and days_between_payment_and_account_creation >= 0 then 0 -- Mastercard Tango
           when cbp.ARN like '7%'and days_between_payment_and_account_creation >= 0 then 0 -- Visa
           when cbp.ARN like '2%'and days_between_payment_and_account_creation >= 0 then 0 -- Visa
           when cbp.ARN like '8%'and days_between_payment_and_account_creation >= 0 then 0 -- Mastercard
           when cbp.ARN like '0%'and days_between_payment_and_account_creation >= 0 then 0 -- Mastercard
           --when cbp.ARN like '9%'and days_between_payment_and_account_creation >= 0 then 0 -- Mastercard
           else 0
       end as weight_3,
       --#4
       case
           when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and LOWER(cbp.full_name) = LOWER(cbp.full_acc_name) then 0.15 -- Mastercard TANGO
           when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and LEFT (LOWER(cbp.first_name),2) = LEFT (LOWER(cbp.first_name_card),2) and LOWER(cbp.last_name) = LOWER(cbp.last_name_card) then 0.15 -- Mastercard
           when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and LEFT (LOWER(cbp.first_name),1) = LEFT (LOWER(cbp.first_name_card),1) and LOWER(cbp.last_name) = LOWER(cbp.last_name_card) then 0.15 -- Mastercard
           when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and LENGTH (cbp.last_name)<=2 and LOWER(cbp.first_name) = LOWER(cbp.first_name_card) and LEFT (LOWER(cbp.last_name),2) = LEFT (LOWER(cbp.last_name_card),2) then 0.15 -- Mastercard
           when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and LENGTH (cbp.last_name)>=3 and LOWER(cbp.first_name) = LOWER(cbp.first_name_card) and LEFT (LOWER(cbp.last_name),3) = LEFT (LOWER(cbp.last_name_card),3) then 0.15 -- Mastercard
           when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and LOWER(cbp.first_name) = LOWER(cbp.last_name_card) and LOWER(cbp.first_name_card) = LOWER(cbp.last_name) then 0.15 -- Mastercard

           when cbp.ARN like '7%' and LOWER(cbp.full_name) = LOWER(cbp.full_acc_name) then 0.1 -- Visa
           when cbp.ARN like '7%' and LEFT (LOWER(cbp.first_name),2) = LEFT (LOWER(cbp.first_name_card),2) and LOWER(cbp.last_name) = LOWER(cbp.last_name_card) then 0.1 -- Visa
           when cbp.ARN like '7%' and LEFT (LOWER(cbp.first_name),1) = LEFT (LOWER(cbp.first_name_card),1) and LOWER(cbp.last_name) = LOWER(cbp.last_name_card) then 0.1 -- Visa
           when cbp.ARN like '7%' and LENGTH (cbp.last_name)<=2 and LOWER(cbp.first_name) = LOWER(cbp.first_name_card) and LEFT (LOWER(cbp.last_name),2) = LEFT (LOWER(cbp.last_name_card),2) then 0.1 -- Visa
           when cbp.ARN like '7%' and LENGTH (cbp.last_name)>=3 and LOWER(cbp.first_name) = LOWER(cbp.first_name_card) and LEFT (LOWER(cbp.last_name),3) = LEFT (LOWER(cbp.last_name_card),3) then 0.1 -- Visa
           when cbp.ARN like '7%' and LOWER(cbp.first_name) = LOWER(cbp.last_name_card) and LOWER(cbp.first_name_card) = LOWER(cbp.last_name) then 0.1 -- Visa

           when cbp.ARN like '2%' and LOWER(cbp.full_name) = LOWER(cbp.full_acc_name) then 0.1 -- Visa
           when cbp.ARN like '2%' and LEFT (LOWER(cbp.first_name),2) = LEFT (LOWER(cbp.first_name_card),2) and LOWER(cbp.last_name) = LOWER(cbp.last_name_card) then 0.1 -- Visa
           when cbp.ARN like '2%' and LEFT (LOWER(cbp.first_name),1) = LEFT (LOWER(cbp.first_name_card),1) and LOWER(cbp.last_name) = LOWER(cbp.last_name_card) then 0.1 -- Visa
           when cbp.ARN like '2%' and LENGTH (cbp.last_name)<=2 and LOWER(cbp.first_name) = LOWER(cbp.first_name_card) and LEFT (LOWER(cbp.last_name),2) = LEFT (LOWER(cbp.last_name_card),2) then 0.1 -- Visa
           when cbp.ARN like '2%' and LENGTH (cbp.last_name)>=3 and LOWER(cbp.first_name) = LOWER(cbp.first_name_card) and LEFT (LOWER(cbp.last_name),3) = LEFT (LOWER(cbp.last_name_card),3) then 0.1 -- Visa
           when cbp.ARN like '2%' and LOWER(cbp.first_name) = LOWER(cbp.last_name_card) and LOWER(cbp.first_name_card) = LOWER(cbp.last_name) then 0.1 -- Visa

           when cbp.ARN like '8%' and LOWER(cbp.full_name) = LOWER(cbp.full_acc_name) then 0.15 -- Mastercard
           when cbp.ARN like '8%' and LEFT (LOWER(cbp.first_name),2) = LEFT (LOWER(cbp.first_name_card),2) and LOWER(cbp.last_name) = LOWER(cbp.last_name_card) then 0.15 -- Mastercard
           when cbp.ARN like '8%' and LEFT (LOWER(cbp.first_name),1) = LEFT (LOWER(cbp.first_name_card),1) and LOWER(cbp.last_name) = LOWER(cbp.last_name_card) then 0.15 -- Mastercard
           when cbp.ARN like '8%' and LENGTH (cbp.last_name)<=2 and LOWER(cbp.first_name) = LOWER(cbp.first_name_card) and LEFT (LOWER(cbp.last_name),2) = LEFT (LOWER(cbp.last_name_card),2) then 0.15 -- Mastercard
           when cbp.ARN like '8%' and LENGTH (cbp.last_name)>=3 and LOWER(cbp.first_name) = LOWER(cbp.first_name_card) and LEFT (LOWER(cbp.last_name),3) = LEFT (LOWER(cbp.last_name_card),3) then 0.15 -- Mastercard
           when cbp.ARN like '8%' and LOWER(cbp.first_name) = LOWER(cbp.last_name_card) and LOWER(cbp.first_name_card) = LOWER(cbp.last_name) then 0.15 -- Mastercard

           when cbp.ARN like '0%' and LOWER(cbp.full_name) = LOWER(cbp.full_acc_name) then 0.15 -- Mastercard
           when cbp.ARN like '0%' and LEFT (LOWER(cbp.first_name),2) = LEFT (LOWER(cbp.first_name_card),2) and LOWER(cbp.last_name) = LOWER(cbp.last_name_card) then 0.15 -- Mastercard
           when cbp.ARN like '0%' and LEFT (LOWER(cbp.first_name),1) = LEFT (LOWER(cbp.first_name_card),1) and LOWER(cbp.last_name) = LOWER(cbp.last_name_card) then 0.15 -- Mastercard
           when cbp.ARN like '0%' and LENGTH (cbp.last_name)<=2 and LOWER(cbp.first_name) = LOWER(cbp.first_name_card) and LEFT (LOWER(cbp.last_name),2) = LEFT (LOWER(cbp.last_name_card),2) then 0.15 -- Mastercard
           when cbp.ARN like '0%' and LENGTH (cbp.last_name)>=3 and LOWER(cbp.first_name) = LOWER(cbp.first_name_card) and LEFT (LOWER(cbp.last_name),3) = LEFT (LOWER(cbp.last_name_card),3) then 0.15 -- Mastercard
           when cbp.ARN like '0%' and LOWER(cbp.first_name) = LOWER(cbp.last_name_card) and LOWER(cbp.first_name_card) = LOWER(cbp.last_name) then 0.15 -- Mastercard

           when partner is not null and cbp.ARN like '7%' and name_on_instr_to_name_in_valid in ('full_match', 'partial_match') then 0.1 -- Visa
           when partner is not null and cbp.ARN like '2%' and name_on_instr_to_name_in_valid in ('full_match', 'partial_match') then 0.1 -- Visa
           when partner is not null and cbp.ARN like '8%' and name_on_instr_to_name_in_valid in ('full_match', 'partial_match') then 0.15 -- Mastercard
           when partner is not null and cbp.ARN like '0%' and name_on_instr_to_name_in_valid in ('full_match', 'partial_match') then 0.15 -- Mastercard
           else 0
       end as weight_4,
       --#5
       case
           when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and (buyer_email_name_match_level in ('full_match', 'partial_match') or partner = 'Paybis-US-broker' and cbp.ARN like '7%' and
                buyer_ekata_email_name_match in ('full_match', 'partial_match')) then 0.2 -- Mastercard Tango
           when cbp.ARN like '7%' and (buyer_email_name_match_level in ('full_match', 'partial_match') or
                buyer_ekata_email_name_match in ('full_match', 'partial_match')) then 0.15 -- Visa
           when cbp.ARN like '2%' and (buyer_email_name_match_level in ('full_match', 'partial_match') or
                buyer_ekata_email_name_match in ('full_match', 'partial_match')) then 0.15 -- Visa
           when cbp.ARN like '8%' and (buyer_email_name_match_level in ('full_match', 'partial_match') or
                buyer_ekata_email_name_match in ('full_match', 'partial_match')) then 0.2 -- Mastercard
           when cbp.ARN like '0%' and (buyer_email_name_match_level in ('full_match', 'partial_match') or
                buyer_ekata_email_name_match in ('full_match', 'partial_match')) then 0.2 -- Mastercard
           else 0
       end as weight_5,
       --#6
       case
            when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and user_has_approved_kyc_from_broker is true or partner = 'Paybis-US-broker' and cbp.ARN like '7%' and user_verified_by_selfie is true then 0.1 -- Mastercard tango
            when cbp.ARN like '7%' and user_has_approved_kyc_from_broker is true or user_verified_by_selfie is true then 0 -- Visa
            when cbp.ARN like '2%' and user_has_approved_kyc_from_broker is true or user_verified_by_selfie is true then 0 -- Visa
            when cbp.ARN like '8%' and user_has_approved_kyc_from_broker is true or user_verified_by_selfie is true then 0.1 -- Mastercard
            when cbp.ARN like '0%' and user_has_approved_kyc_from_broker is true or user_verified_by_selfie is true then 0.1 -- Mastercard
           else 0
       end as weight_6,
       --#7
       --  buyer_device_type,
       --  buyer_device_brand,--normalized
       --  buyer_device_family,
       --  buyer_device_model,
       --  buyer_mm_device_id,
       --#8
      case
           when fr.is_in_fraud_ring then 1
           else 0
       end as weight_8,
       --#9
       case

           when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and avs_match then 0.2 --Mastercard tango
           when cbp.ARN like '7%'and avs_match then 0.25 --Visa
           when cbp.ARN like '2%'and avs_match then 0.25 --Visa
           when cbp.ARN like '8%'and avs_match then 0.2 --Mastercard
           when cbp.ARN like '0%'and avs_match then 0.2 --Mastercard
           --when cbp.ARN like '9%'and avs_match then 0.2 --Mastercard
           else 0
       end as weight_9,
       --#10
      case
          when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and (buyer_ekata_address_phone_match in ('full_match', 'partial_match') or
                buyer_ekata_name_phone_match in ('full_match', 'partial_match') or
                buyer_phone_name_to_card_name_match_ea_ekata in ('full_match', 'partial_match')) then 0.15 -- Mastercard tango
           when cbp.ARN like '7%' and (buyer_ekata_address_phone_match in ('full_match', 'partial_match') or
                buyer_ekata_name_phone_match in ('full_match', 'partial_match') or
                buyer_phone_name_to_card_name_match_ea_ekata in ('full_match', 'partial_match')) then 0.15 -- Visa
           when cbp.ARN like '2%' and (buyer_ekata_address_phone_match in ('full_match', 'partial_match') or
                buyer_ekata_name_phone_match in ('full_match', 'partial_match') or
                buyer_phone_name_to_card_name_match_ea_ekata in ('full_match', 'partial_match')) then 0.15 -- Visa
           when cbp.ARN like '8%' and (buyer_ekata_address_phone_match in ('full_match', 'partial_match') or
                buyer_ekata_name_phone_match in ('full_match', 'partial_match') or
                buyer_phone_name_to_card_name_match_ea_ekata in ('full_match', 'partial_match')) then 0.15 -- Mastercard
            when cbp.ARN like '0%' and (buyer_ekata_address_phone_match in ('full_match', 'partial_match') or
                buyer_ekata_name_phone_match in ('full_match', 'partial_match') or
                buyer_phone_name_to_card_name_match_ea_ekata in ('full_match', 'partial_match')) then 0.15 -- Mastercard
           else 0
       end as weight_10,
       --#11
       case
           when days_txn_cb > 120 then 1
           else 0
       end as weight_11,
       -- Check if the gap between the CB processing date and the CB posting date is less than 120 days – if the gap is bigger the 120 days – CB is invalid (Exception – the query should break and return 100)
       --#12 -- should it be about the card?
       --case
       --    when card_num_chargebacks > 0 then 1
       --    else 0
       --end as weight_12,
       --#13
       case
           when not has_fraud_warning then 0
           else 0
       end as weight_13,
       --#14
       case
           when partner = 'Paybis-US-broker' and cbp.ARN like '7%' and num_new_btc_address_users > 0 and buyer_num_approved_payments > 0 and buyer_num_chargebacks = 0 then 0 -- Mastercard tango
           when cbp.ARN like '7%'and num_new_btc_address_users > 0 and buyer_num_approved_payments > 0 and buyer_num_chargebacks = 0 then 0 -- Visa
           when cbp.ARN like '2%'and num_new_btc_address_users > 0 and buyer_num_approved_payments > 0 and buyer_num_chargebacks = 0 then 0 -- Visa
           when cbp.ARN like '8%'and num_new_btc_address_users > 0 and buyer_num_approved_payments > 0 and buyer_num_chargebacks = 0 then 0 -- Mastercard
           when cbp.ARN like '0%'and num_new_btc_address_users > 0 and buyer_num_approved_payments > 0 and buyer_num_chargebacks = 0 then 0 -- Mastercard
           else 0
       end as weight_14

       --#15
       --case
       --    when was_auth_done_with_threeds is true and cbp.ARN not like '2%' then 1
       --    else 0
       --end as weight_15
       -- special case 0-Hash
       --case
       --    when cbp.ARN like '2%' then true
       --    else false
       --end as is_0hash

   from
       cb_payments as cbp
       left join dvars as d on cbp.payment_id = d.payment_id
       left join others as o on d.payment_id = o.payment_id
       left join fraud_rings as fr on cbp.payment_id = fr.payment_id
       left join cb_transaction_days as cbtd on cbp.payment_id = cbtd.payment_id
       --left join card_chargebacks as ccb on cbp.payment_id = ccb.payment_id
       left join avs on cbp.payment_id = avs.payment_id
       left join Count_Undisputed on cbp.cc_identifier = Count_Undisputed.cc_identifier
       left join Count_Undisputed_apple_pay on cbp.Last4_CC = Count_Undisputed_apple_pay.cc_identifier
       )
select
   payment_id,
       --when (weight_8 = 1)  and (weight_1 + weight_2 + weight_3 + weight_4 + weight_5 + weight_9 + weight_10 + weight_13 + weight_14 + weight_6) < 0.4  then 0 -- Gives score 0 for fraud rings with weak evidence under 0.4
       --when (weight_11 = 1 or weight_12 = 1 or weight_14 = 1) and not is_0hash then 1
       --when (weight_11 = 1 ) then 1
    weight_1 + weight_2 + weight_3 + weight_4 + weight_5 + weight_9 + weight_10 + weight_13 + weight_14 + weight_6 as rank,
    weight_6 as KYC,
    weight_1 as ip_match,
    weight_4 as cc_owner_match,
    --weight_3 as profile_before_trx,
    weight_9 as AVS_match,
    weight_10 as phone_match,
    weight_5 as ekata_email_match,
    weight_2 as undisputed_past_trx
    --weight_14 as same_crypto_address_past_trx,
    --weight_13 as No_FW,
    --weight_8 as fraud_ring,
    --weight_11 as invalid_dispute_dates_more_than_120


   from weights
order by rank desc
), link_table_1 as (
   SELECT *
   FROM (
            VALUES ('AAVE', 'https://etherscan.io/tx/'),
                   ('ADA', 'https://blockchair.com/cardano/transaction/'),
                   ('ALGO', 'https://algoexplorer.io/tx/'),
                   ('APE', 'https://etherscan.io/tx/'),
                   ('ATOM', 'https://atom.tokenview.com/en/tx/'),
                   ('AVAX', 'https://explorer-xp.avax.network/tx/'),
                   ('AVAX-C', 'https://snowtrace.io/tx/'),
                   ('BABYDOGE', 'https://bscscan.com/txs?a='),
                   ('BAT', 'https://etherscan.io/tx/'),
                   ('BSV', 'https://blockchair.com/bitcoin-sv/address/'),
                   ('BTC', 'https://blockchair.com/bitcoin/address/'),
                   ('BCH','https://blockchair.com/bitcoin-cash/address/'),
                   ('BUSD', 'https://etherscan.io/tx/'),
                   ('BUSD-SC', 'https://bscscan.com/tx/'),
                   ('CAKE', 'https://bscscan.com/tx/'),
                   ('CEL', 'https://etherscan.io/tx/'),
                   ('COTI', 'https://explorer.coti.io/'),
                   ('CRO-ERC20', 'https://etherscan.io/tx/'),
                   ('CUSD', 'https://explorer.celo.org/tx/'),
                   ('DAI', 'https://etherscan.io/tx/'),
                   ('DASH', 'https://blockchair.com/dash/address/'),
                   ('DGB', 'https://dgb.tokenview.com/en/address/'),
                   ('DOGE', 'https://blockchair.com/dogecoin/address/'),
                   ('DOT', 'https://blockchair.com/polkadot/address/'),
                   ('ELON', 'https://etherscan.io/tx/'),
                   ('EOS', 'https://bloks.io/transaction/'),
                   ('ETH', 'https://etherscan.io/tx/'),
                   ('FIL', 'https://filfox.info/en/address/'),
                   ('GALA', 'https://etherscan.io/tx/'),
                   ('HBAR', 'https://app.dragonglass.me/hedera/transactions/'),
                   ('ICX', 'https://tracker.icon.foundation/transaction/'),
                   ('KAI', 'https://explorer.kardiachain.io/tx/'),
                   ('KAVA', 'https://www.mintscan.io/kava/txs/'),
                   ('KCS', 'https://etherscan.io/tx/'),
                   ('KLV', 'https://tronscan.org/#/transaction/'),
                   ('KSM', 'https://kusama.polkastats.io/extrinsic/'),
                   ('LINK', 'https://etherscan.io/tx/'),
                   ('LTC', 'https://blockchair.com/litecoin/address/'),
                   ('LUNA', 'https://finder.terra.money/mainnet/address/'),
                   ('MATIC', 'https://polygonscan.com/tx/'),
                   ('NANO', 'https://nanolooker.com/block/'),
                   ('NEAR', 'https://nearblocks.io/txns/'),
                   ('ONE', 'https://explorer.harmony.one/tx/'),
                   ('OSMO', 'https://www.mintscan.io/osmosis/account/'),
                   ('QTUM', 'https://explorer.qtum.org/address/'),
                   ('RVN', 'https://rvn.tokenview.com/en/address/'),
                   ('SAND', 'https://etherscan.io/tx/'),
                   ('SHIB', 'https://etherscan.io/tx/'),
                   ('SOL', 'https://solscan.io/tx/'),
                   ('TRX', 'https://tronscan.org/#/transaction/'),
                   ('UNI', 'https://etherscan.io/tx/'),
                   ('USDC', 'https://etherscan.io/tx/'),
                   ('USDC-TRC20', 'https://tronscan.org/#/transaction/'),
                   ('USDK', 'https://etherscan.io/tx/'),
                   ('USDT', 'https://etherscan.io/tx/'),
                   ('USDT-TRC20', 'https://tronscan.org/#/transaction/'),
                   ('VET', 'https://explorer.vtho.net/#/transactions/'),
                   ('WAXP', 'https://wax.bloks.io/transaction/'),
                   ('XAUT', 'https://etherscan.io/tx/'),
                   ('XDC', 'https://explorer.xinfin.network/txs/'),
                   ('XEM', 'https://explorer.nemtool.com/#/s_tx?hash='),
                   ('XLM', 'https://stellarchain.io/tx/'),
                   ('XRP', 'https://blockexplorer.one/xrp/mainnet/tx/'),
                   ('XTZ', 'https://tzstats.com/'),
                   ('HTR', 'https://explorer.hathor.network/transaction/')
        ) as a (coin, prefix_link)
), link_table_2 as (
   select * from (values ('BNB','https://explorer.binance.org/tx/')) as b (coin_BNB, prefix_link)
), link_table_3 as (
   select * from (values ('BNB','https://bscscan.com/tx/'),
                         ('BNB-SC','https://bscscan.com/tx/')
                 ) as c (coin_BSC, prefix_link)
), peer as (
   select distinct pel.payment_id, --p.bin,
                   blockchain_txn_hash
   from Upload_to_sf
            join payment_events_log as pel on Upload_to_sf.Simplex_ID = pel.payment_id
            --left join payments as p on p.payment_id = pel.payment_id
            left join payment_execution_event_response as peer on pel.id = peer.event_id
   where blockchain_txn_hash is not null
), final as (
select
   p.id,
   case
       when coin in ('BABYDOGE','BCH','BSV','BTC','DASH','DGB','DOGE','DOT','FIL','LTC','LUNA','OSMO','QTUM','RVN') then concat(lt1.prefix_link, p.crypto_currency_address)
       when coin in ('ETH','AAVE','ADA','ALGO','APE','ATOM','AVAX','AVAX-C','BAT','BUSD','BUSD-SC','CAKE','COTI','CUSD','ELON','EOS','GALA','ICX','KAI','KAVA','KCS','KLV','KSM','MATIC','NANO','NEAR','ONE','SAND','SHIB','SOL','TRX','USDC','USDC-TRC20','USDK','USDT','USDT-TRC20','VET','WAXP','XAUT','XDC','XEM','XLM','XRP','XTZ', 'HTR') then concat(lt1.prefix_link, peer.blockchain_txn_hash)
       when coin in ('CEL', 'CRO-ERC20','DAI','LINK','UNI') then concat(lt1.prefix_link, '0x', peer.blockchain_txn_hash)
       when coin_BNB in ('BNB') and p.crypto_currency_address not like '0x%' then concat(lt2.prefix_link, peer.blockchain_txn_hash)
       when coin_BSC in ('BNB','BNB-SC') and (p.crypto_currency_address like '0%') then concat(lt3.prefix_link, peer.blockchain_txn_hash)
       when coin in ('HBAR') then concat(lt1.prefix_link, regexp_replace(peer.blockchain_txn_hash,'[-.]','','g'))
       else 'error' end as blockchain_link
from peer
        left join payments p on p.id = peer.payment_id
        left join link_table_1 lt1 on p.crypto_currency = lt1.coin
        left join link_table_2 lt2 on p.crypto_currency = lt2.coin_BNB
        left join link_table_3 lt3 on p.crypto_currency = lt3.coin_BSC
where crypto_currency_address is not null
), AVS_code as (
   select distinct p.payment_id,
           case
           when pr.processor = 'ecp' and pr.raw_response ->> 'avs_response_code' in ('5A', '5B', '5D', '5F', '5M', '5P', '5W', '5X', '5Y', '5Z') then true
           when pr.processor = 'paysafe' and pr.raw_response ->> 'avsResponse' in ('MATCH', 'MATCH_ADDRESS_ONLY', 'MATCH_ZIP_ONLY') then true
           when pr.processor = 'safecharge' and pr.raw_response #>> '{paymentOption, card, avsCode}' in ('A', 'B', 'D', 'F', 'M', 'P', 'W', 'X', 'Y', 'Z') then true
           --when pr.processor = 'worldline' and pr.raw_response #>> '{POS-GWLink, AuthorisationResponse, Transaction, HostResponse, AVSResponseCode, _text}' in ('00', '02') then true
           else false
       end as AVS
    from proc_requests pr join peer p on p.payment_id = pr.payment_id
    join chargebacks as cb on pr.payment_id = cb.payment_id
    where pr.tx_type = 'authorization' and pr.status='success'
and cb.inserted_at > current_date - 5
), AVS_notsent as (
   select distinct p.payment_id,
           case
           when pr.processor = 'ecp' and pr.raw_response ->> 'avs_response_code' in ('5G', '5R', '5U') then true
           when pr.processor = 'safecharge' and pr.raw_response #>> '{paymentOption, card, avsCode}' in ('R', 'G', 'U') then true
           else false
               end as avs_check
    from proc_requests pr join peer p on p.payment_id = pr.payment_id
    where pr.tx_type = 'authorization' and pr.status='success'
), AVS_value as (
   select distinct p.payment_id, case
    when processor = 'ecp' then RIGHT(raw_response ->> 'avs_response_code',1)
    when processor = 'paysafe' then raw_response ->> 'avsResponse'
    when processor = 'safecharge' then raw_response #>> '{paymentOption, card, avsCode}'
    --when processor = 'worldline' then raw_response #>> '{POS-GWLink, AuthorisationResponse, Transaction, HostResponse, AVSResponseCode, _text}'
    else 'Unknown' end as avs_value
  from proc_requests pr join peer p on p.payment_id = pr.payment_id
  join chargebacks as cb on pr.payment_id = cb.payment_id
  where   pr.status = 'success'  AND pr.tx_type = 'authorization'
  and cb.inserted_at > current_date - 5
), threeds as (
   select distinct proc_requests.payment_id as pid,
                   case when threeds #>> '{cavv}' is not null then threeds #>> '{eci}'
                       else '' end as eci,
                   threeds #>> '{cavv}'     as cavv,
                   credit_card,
                   concat(raw_response #>> '{authorization_code}', raw_response #>> '{authCode}', SUBSTRING(
                               raw_response #>>
                               '{POS-GWLink,AuthorisationResponse,Transaction,HostResponse,AuthorisationCode}', 12,
                               6))          as auth_code
   from proc_requests
            join peer p on p.payment_id = proc_requests.payment_id
            join cc_identifiers on proc_requests.cc_identifier = cc_identifiers.identifier
   where proc_requests.threeds is not null
     and proc_requests.status = 'success'
     and threeds #>> '{cavv}' is not null
), threeds_zero as (
    select distinct
        pr.payment_id as pid,
        raw_response -> 'paymentOption' -> 'card' -> 'threeD' ->> 'eci' as auth_zero
        from proc_requests as pr
            join peer p on p.payment_id = pr.payment_id
            join chargebacks as cb on pr.payment_id = cb.payment_id
        where pr.threeds is not null
          and pr.status='success'
          and threeds#>>'{cavv}' is not null
        and cb.inserted_at > current_date - 5
),
    fraud_rings as (
    select distinct fr.payment_id, case when fr.third_party_scam is not null then 'Yes' end as fraud_ring_val, fr.tag
        from arnd.mv_fraud_ring_tags as fr
), fraud_types as (
         select distinct pr.payment_id,
        case
           when pr.processor = 'ecp' and fraud_type_code = '0' then '0'
           when pr.processor = 'safecharge' and fw.raw_data #>> '{fraudType}' = 'Lost Fraud' then '0'
           when pr.processor = 'ecp' and fraud_type_code = '1' then '1'
           when pr.processor = 'safecharge' and fw.raw_data #>> '{fraudType}' = 'Stolen Fraud' then '1'
           when pr.processor = 'safecharge' and fw.raw_data #>> '{fraudType}' = 'Never Received Issue' then '2'
           when pr.processor = 'ecp' and fraud_type_code = '3' then '3'
           when pr.processor = 'safecharge' and fw.raw_data #>> '{fraudType}' = 'Fraudulent Application' then '3'
           when pr.processor = 'ecp' and fraud_type_code = '4' then '4'
           when pr.processor = 'safecharge' and fw.raw_data #>> '{fraudType}' = 'Counterfeit Card Fraud' then '4'
-------------------------------------------------------------------------------------------------
           when pr.processor = 'ecp' and coalesce((cb.raw_data #>> '{arn}'), (cb.raw_data #>> '{ARN}'),
           (cb.raw_data #>> '{Acquirer ref number}'),
           (cb.raw_data #>> '{Acquirer Ref Number}'))  like '7%' and fraud_type_code = '5' then 'V5'

            when pr.processor = 'ecp' and coalesce((cb.raw_data #>> '{arn}'), (cb.raw_data #>> '{ARN}'),
           (cb.raw_data #>> '{Acquirer ref number}'),
           (cb.raw_data #>> '{Acquirer Ref Number}'))  like '2%' and fraud_type_code = '5' then 'V5'

           when pr.processor = 'ecp' and coalesce((cb.raw_data #>> '{arn}'), (cb.raw_data #>> '{ARN}'),
           (cb.raw_data #>> '{Acquirer ref number}'),
           (cb.raw_data #>> '{Acquirer Ref Number}'))  like '8%' and fraud_type_code = '5' then 'M5'

            when pr.processor = 'ecp' and coalesce((cb.raw_data #>> '{arn}'), (cb.raw_data #>> '{ARN}'),
           (cb.raw_data #>> '{Acquirer ref number}'),
           (cb.raw_data #>> '{Acquirer Ref Number}'))  like '0%' and fraud_type_code = '5' then 'M5'

            when pr.processor = 'ecp' and coalesce((cb.raw_data #>> '{arn}'), (cb.raw_data #>> '{ARN}'),
           (cb.raw_data #>> '{Acquirer ref number}'),
           (cb.raw_data #>> '{Acquirer Ref Number}'))  like '9%' and fraud_type_code = '5' then 'M5'

           when pr.processor = 'safecharge' and coalesce((cb.raw_data #>> '{arn}'), (cb.raw_data #>> '{ARN}'),
           (cb.raw_data #>> '{Acquirer ref number}'),
           (cb.raw_data #>> '{Acquirer Ref Number}'))  like '7%' and fw.raw_data #>> '{fraudType}' = 'Other' then 'V5'

            when pr.processor = 'safecharge' and coalesce((cb.raw_data #>> '{arn}'), (cb.raw_data #>> '{ARN}'),
           (cb.raw_data #>> '{Acquirer ref number}'),
           (cb.raw_data #>> '{Acquirer Ref Number}'))  like '2%' and fw.raw_data #>> '{fraudType}' = 'Other' then 'V5'

           when pr.processor = 'safecharge' and coalesce((cb.raw_data #>> '{arn}'), (cb.raw_data #>> '{ARN}'),
           (cb.raw_data #>> '{Acquirer ref number}'),
           (cb.raw_data #>> '{Acquirer Ref Number}'))  like '8%' and fw.raw_data #>> '{fraudType}' = 'Account Takeover Fraud' then 'M5'

            when pr.processor = 'safecharge' and coalesce((cb.raw_data #>> '{arn}'), (cb.raw_data #>> '{ARN}'),
           (cb.raw_data #>> '{Acquirer ref number}'),
           (cb.raw_data #>> '{Acquirer Ref Number}'))  like '0%' and fw.raw_data #>> '{fraudType}' = 'Account Takeover Fraud' then 'M5'

            when pr.processor = 'safecharge' and coalesce((cb.raw_data #>> '{arn}'), (cb.raw_data #>> '{ARN}'),
           (cb.raw_data #>> '{Acquirer ref number}'),
           (cb.raw_data #>> '{Acquirer Ref Number}'))  like '9%' and fw.raw_data #>> '{fraudType}' = 'Account Takeover Fraud' then 'M5'
-------------------------------------------------------------------------------------------------
           when pr.processor = 'ecp' and fraud_type_code = '6' then '6'
           when pr.processor = 'ecp' and fraud_type_code = 'NaN' then 'D'
           --when pr.processor = 'paysafe' and fraud_type_code = '6' then '6'
           when pr.processor = 'safecharge' and fw.raw_data #>> '{fraudType}' = 'Card Not Present Fraud' then '6'
           when pr.processor = 'safecharge' and fw.raw_data #>> '{fraudType}' = 'Processing Error' then 'A'
           when pr.processor = 'safecharge' and fw.raw_data #>> '{fraudType}' = 'Account Takeover Fraud' then 'B'
           when pr.processor = 'safecharge' and fw.raw_data #>> '{fraudType}' = 'Unknown' then 'Unknown'
           --when fw.raw_data #>> '{fraudType}' = 'null' AND fraud_type_code = 'null' then 'Unknown'
           else 'null'
               end as fraud_type
        from proc_requests pr
        left join peer p on p.payment_id = pr.payment_id
        left join fraud_warnings fw on fw.payment_id = pr.payment_id
        left join chargebacks as cb on pr.payment_id = cb.payment_id
        where pr.tx_type = 'authorization' and pr.status='success'
        and cb.inserted_at > current_date - 5
), dss_details as (
    select distinct on(p.id) p.id,
       concat(seu.first_name,' ', seu.last_name) as full_acc_name,
       seu.created_at as registration_date,
       ube.request_data #>> '{session, ip}' as ip_address,
       concat(em.data #>> '{ip_address ,city, names, en}',' ', em.data #>> '{ip_address ,subdivisions, 0, names, en}',' ', em.data #>> '{ip_address ,postal, code}',' ', em.data #>> '{ip_address ,country, names, en}') as ip_location,
       case
           when (em.data #>> '{billing_address, distance_to_ip_location}')::varchar = '0' then '<10'
           else (em.data #>> '{billing_address, distance_to_ip_location}')::varchar
           end as bill_add_dist,
       (em.data #>> '{ip_address, location, accuracy_radius}')::numeric as accuracy_radius,
       (em.data #>> '{ip_address, risk}')::numeric as proxy_score,
       device_id
         from payments as p
         left join r_payments r ON p.id = r.simplex_payment_id
         left join chargebacks as cb on p.id = cb.payment_id
         left join maxmind_triggers mt on r.id = mt.r_payment_id
         left join enrich_maxmind em ON mt.enrich_maxmind_id = em.id
         left join simplex_end_users as seu on p.simplex_end_user_id = seu.id
         left join user_browser_events ube on ube.payment_id = p.id
         where ube.event_type ='authorize'
         and cb.inserted_at > current_date - 5
), cc_digits as (
        select distinct on(p.id) p.id, case
    when p.bin like '4%' then right(credit_card,4)
    when p.bin like '2%' then right(credit_card,4)
    when p.bin like '5%' then right(credit_card,4)
    when p.bin like '6%' then right(credit_card,4)
    when p.bin is null and c.raw_data#>>'{paymentMethod}' like 'Visa' then  right(appa.raw_payment_method_display_name,4)
    when p.bin is null and c.raw_data#>>'{paymentMethod}' like 'Master%' then  right(appa.raw_payment_method_display_name,4)
    else right(appa.raw_payment_method_display_name,4) end as last_4_CC_digits --Pakeitem Error i AP table
        from payments p
        left join payment_credit_cards pcc on p.id = pcc.payment_id
        left join cc_identifiers as cc on p.cc_identifier = cc.identifier
        left join proc_requests as pr on pr.payment_id = p.id
        left join credit_cards.apple_pay_payment_attempts appa on pcc.credit_card_id::text = appa.payment_instrument_id::text
        left join chargebacks as c on p.id = c.payment_id
        where pr.tx_type = 'authorization' and pr.status='success' and c.inserted_at > '2022-01-01' and pr.payment_instrument_type <> 'google_pay'
        group by p.id, last_4_CC_digits
union
select distinct on(p.id) p.id, pr.raw_response#>> '{last4Digits}' as last_4_CC_digits
from proc_requests pr join payments p on p.id = pr.payment_id where (pr.raw_response#>> '{last4Digits}', simplex_end_user_id) in (
 ( select distinct pr.raw_response#>> '{last4Digits}' , p.simplex_end_user_id
from payments p join proc_requests pr on p.id = pr.payment_id
where p.id in (select distinct c.payment_id from chargebacks as c where c.inserted_at >'2022-01-01') and pr.tx_type = 'capture-authorization' and pr.payment_instrument_type = 'google_pay')) and p.status in (2) and pr.payment_instrument_type = 'google_pay' --and pr.status='success'
group by p.id, last_4_CC_digits
)

select    cb_template            AS Chargeback_Template__c,
          fight_until::date      AS Fight_Until__c,
          arn                    AS Name,
          simplex_id             AS Simplex_ID__c,
          credit_card_identifier AS CC_Identifier__c,
          analysts_decision      AS Fraud_Type__c,
          tag                    AS Fraud_Ring__c,
          sub_classification     AS Sub_Fraud_Type__c,
          crypto_curr            AS Crypto_Currency__c,
          partner_name           AS Partner_Name__c,
          account_id             AS Partner_Account_Number__c,
          threeds                AS X3DS__c,
          threeds.eci            AS ECI__c,
          has_fw                 AS Has_FW__c,
          was_refunded           AS Was_Refunded__c,
          cb_type                AS Chargeback_Type__c,
          reason_code            AS Reason_Code__c,
          reason_description     AS Reason_Description__c,
          pay_amt                AS Payment_Amount__c,
          pay_curr               AS Payment_Currency__c,
          cb_amt                 AS CBA_Amount__c,
          cb_amt_curr            AS CB_Currency__c,
          pay_amt_usd            AS Payment_Amount_USD__c,
          transaction_date       AS Transaction_Date__c,
          cb_posting_date        AS Chargeback_Posting_Date__c,
          final_decision_date    AS Final_Decision_Date__c,
          status                 AS Status__c,
          stage                  AS Stage__c,
          decision               AS Decision__c,
          processor              AS ARN_not_used__c,
          is_simplex_liable      AS Is_Simplex_Liable__c,
          --card_present           AS Card_Present__c,
          card_scheme            AS Card_Scheme__c,
          issuing_bank           AS Issuing_Bank__c,
          bin_country            AS Country__c,
          partner_id             AS Partner_ID__c,
          simplex_long_id        AS Simplex_Long_ID__c,
          total_amt_by_card      AS Total_Amount_By_Card__c,
          uti                    AS UTI__c,
          mid                    AS MID__c,
          descriptor             AS Descriptor__c,
          "turnkey/exchange"     AS Turnkey_or_Partner_Name__c,
          Upload_to_SF."TK ID"   AS Turnkey_or_Partner_ID__c,
          crypto_address         AS Crypto_Address__c,
          crypto_amount          AS Crypto_Amount__c,
          rank                   AS Identity_Match__c,
          full_acc_name          AS Chargeback_Account_Name__c, --new field (DSS to Forms)
          registration_date      AS Registration_Date__c, --new field (DSS to Forms)
          ip_address             AS IP_Address__c,  --new field (DSS to Forms)
          ip_location            AS IP_Location__c,  --new field (DSS to Forms)
          bill_add_dist          AS Billing_Address_Distance__c,  --new field (DSS to Forms)
          accuracy_radius        AS Accuracy_Radius__c,  --new field (DSS to Forms)
          proxy_score            AS Proxy_Score__c,  --new field (DSS to Forms)
          device_id              AS Device_Id__c,
          last_4_CC_digits       AS Last_4_CC_Digits__c,
          full_name              AS Full_Name__c, ---field not found in sf
          email                  AS Email__c,     ---field not found in sf
          billing_address        AS Billing_Address__c,       ---field not found in sf
          AB_Fight_Until         AS AB_Fight_Until_Date__c,   ---field not found in sf
          email_confirmation     AS Has_Email_Confirmation__c,
          blockchain_link        AS Blockchain_Website__c,
          phone                  AS Phone__c,
      case when fraud_type = 'null' then 'Unknown'
          else fraud_type end    AS Fraud_Type_code__c,
          avs_value              AS AVS_value__c,
      case when avs in ('true') then 'Yes'
           else 'No' end      AS AVS_Match__c,
      case when avs_check in ('true') then 'Yes'
           else 'No' end      AS AVS_Check__c,
      case
          when ((threeds.eci IN ('1','2','5','6', '01', '02', '05', '06')) and threeds.cavv is not null) and Partner_Name <> 'Paybis-US-broker'
            then concat('ECI ', threeds.eci, ', CAVV: ', threeds.cavv, ', Card Number ', credit_card, ', Auth Code: ', threeds.auth_code)
          when Partner_Name = 'Paybis-US-broker' AND (threeds_zero.auth_zero IN ('1','2','5','6', '01', '02', '05', '06'))
            then concat('ECI ', threeds.eci, ', CAVV: ', threeds.cavv, ', Card Number ', credit_card, ', Auth Code: ', threeds.auth_code)
          else 'False' end      AS X3DS_Details__c
from Upload_to_sf left join Fraud_Query on Upload_to_sf.Simplex_ID = Fraud_Query.payment_id
   left join final on final.id = Upload_to_sf.Simplex_ID
   left join AVS_code on AVS_code.payment_id = Upload_to_sf.Simplex_ID
   left join AVS_notsent on AVS_notsent.payment_id = Upload_to_sf.Simplex_ID
   left join AVS_value on AVS_value.payment_id = Upload_to_sf.Simplex_ID
   left join threeds on threeds.pid = Upload_to_sf.Simplex_ID
   left join threeds_zero on threeds_zero.pid = Upload_to_sf.Simplex_ID
   left join fraud_rings on fraud_rings.payment_id = Upload_to_sf.Simplex_ID
   left join fraud_types on fraud_types.payment_id = Upload_to_sf.Simplex_ID
   left join dss_details on dss_details.id = Upload_to_sf.Simplex_ID
   left join cc_digits on cc_digits.id = Upload_to_sf.Simplex_ID
;"""