import logging
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timezone, timedelta
from functools import partial

from scripts.commons.consts import Consts as cnst
from scripts.commons.logger.SlackLogger import SlackLogger


days = Variable.get("bi_threeds_days", deserialize_json=True)
today_utc = "'" + (datetime.today() - timedelta(days=days)).strftime('%Y-%m-%d') + "'" + '::timestamptz'

def python_funcation():
    return True

# create logger/notifier
env_config = Variable.get("env_config", deserialize_json=True)
error_notifier = SlackLogger(environment=env_config['INFRA'], channel_id=cnst.SNOWFLAKE_MONITOR_PIPE)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    "provide_context": True,
    'retries': 3,  # Number of retries if the task fails
    'retry_delay': timedelta(minutes=1),  # Delay between retries
    'on_failure_callback': partial(error_notifier.send_notification, cnst.ERROR)
}

dag = DAG(
    dag_id="bi_threeds_table", default_args=args, schedule_interval="05/30 * * * *",
    dagrun_timeout=timedelta(minutes=30), max_active_runs=1,
    catchup=False
)

with dag:

    python_task = PythonOperator(
        task_id=f"python_task",
        python_callable=python_funcation
    )

    snowflake_task_delete = SnowflakeOperator(
        task_id=f'snowflake_task_delete',
        snowflake_conn_id="snowflake_conn",
        sql=f"""
        delete from "SIMPLEX_RAW"."BI"."THREEDS"
        WHERE AUTH_REQUEST_CREATED_AT>{today_utc}
            """
    )
    snowflake_task_insert = SnowflakeOperator(
        task_id=f'snowflake_task_insert',
        snowflake_conn_id="snowflake_conn",
        sql=f"""
        insert into bi.threeds
        WITH max_auth_request_id AS (
         SELECT payment_auth_request_threeds.auth_request_id,
            max(payment_auth_request_threeds.created_at) AS created_at
         FROM payment_auth_request_threeds
         where  payment_auth_request_threeds.created_at> {today_utc}
         GROUP BY payment_auth_request_threeds.auth_request_id
        ),
        raw_data AS (
         SELECT par.payment_id, --VV
            coalesce(it.amount,t.amount)                         AS total_amount,   --new change
            coalesce(it.currency_code,t.currency_code)           AS currency,       --new change
            coalesce(it.scheme,t.card_scheme)                    AS card_scheme,    --new change
            par.request_processor_id AS processor_id, --VV
            par.request_processor__name AS processor__name,  
            coalesce(it.merchant_simplex_name,t.simplex_mid_key) AS mid,            --new change
            coalesce(it.credit_card_id,t.credit_card_id)         AS credit_card_id, --new change
            par.created_at AS auth_request_created_at, --VV
                CASE
                    WHEN   tar.cascaded_to_v1 is not null  THEN  '3DS 1.0' --new change
                    WHEN (((par.request_threeds_version = '0') OR (length(par.request_threeds_version) IS NULL)) AND (par.request_skip_threeds = 'false')) THEN 'S2S'
                    WHEN ("left"(par.request_threeds_version, 1) = '1' OR it.version::text='1') THEN '3DS 1.0' --new change
                    WHEN ("left"(par.request_threeds_version, 1) = '2' OR it.version::text='2') THEN '3DS 2.0' --new change
                    WHEN (par.request_skip_threeds = 'false') THEN 'other'
                    ELSE NULL
                END AS flow, --VV
           CASE WHEN (it.id)  IS NOT NULL THEN  'safecharge' ELSE --new change
                 COALESCE(vr.threeds_provider,cpr.threeds_provider, (me.mpi_provider)::text) END AS threeds_provider,
           COALESCE(REGEXP_SUBSTR(tar.acs_url,'/([^/]+)',1,1,'e' ),REGEXP_SUBSTR(ar2.response_acs_url,'/([^/]+)',1,1,'e' )) AS  threeds_issuer, --new change
                CASE
                    WHEN tar.enrollment_status='Y' THEN true --new change
                    WHEN ((par.request_threeds_version = '0') OR (length(par.request_threeds_version) IS NULL)) THEN me.is_enrolled
                    WHEN (("left"(par.request_threeds_version, 1) = '1') AND (cpr2.enrollment = 'Y')) THEN true
                    WHEN (("left"(par.request_threeds_version, 1) = '1') AND (cpr2.enrollment in ('N', 'U'))) THEN false ---change from any to in
                    WHEN (("left"(par.request_threeds_version, 1) = '2') AND (vr2.versioning_requests_id IS NULL)) THEN false
                    WHEN (("left"(par.request_threeds_version, 1) = '2') AND (vr2.errors IS NOT NULL)) THEN false
                    WHEN ("left"(par.request_threeds_version, 1) = '2') THEN true
                    ELSE NULL::boolean
                END AS is_enrolled, --VV
            COALESCE(tar.enrollment_status::text,cpr2.enrollment, (me.enrollment_res)::text) AS enrollment_result, --new change
                CASE
                    WHEN (coalesce(tar.errors,cpr2.errors) <> '[]') THEN true
                    WHEN (("left"(par.request_threeds_version, 1) = '2') AND (vr2.versioning_requests_id IS NULL)) THEN true
                    ELSE false
                END AS enrollment_errors, --VV
                CASE
                    WHEN ("left"(par.request_threeds_version, 1) = '2') THEN COALESCE(tvt.THREEDS_STATUS,r.status, ar2.status)
                    WHEN ("left"(par.request_threeds_version, 1) = '1') THEN COALESCE(tvt.THREEDS_STATUS,pvr2.status)
                    ELSE (mav.three_d_result)::text
                END AS threeds_validation_result, --VV
                CASE
                    WHEN (ar2.reponse_reason = '01') THEN 'Card authentication failed'
                    WHEN (ar2.reponse_reason = '02') THEN 'Unknown Device'
                    WHEN (ar2.reponse_reason = '03') THEN 'Unsupported Device'
                    WHEN (ar2.reponse_reason = '04') THEN 'Exceeds authentication frequency limit'
                    WHEN (ar2.reponse_reason = '05') THEN 'Expired card'
                    WHEN (ar2.reponse_reason = '06') THEN 'Invalid card number'
                    WHEN (ar2.reponse_reason = '07') THEN 'Invalid transaction'
                    WHEN (ar2.reponse_reason = '08') THEN 'No Card record'
                    WHEN (ar2.reponse_reason = '09') THEN 'Security failure'
                    WHEN (ar2.reponse_reason = '10') THEN 'Stolen card'
                    WHEN (ar2.reponse_reason = '11') THEN 'Suspected fraud'
                    WHEN (ar2.reponse_reason = '12') THEN 'Transaction not permitted to cardholder'
                    WHEN (ar2.reponse_reason = '13') THEN 'Cardholder not enrolled in service'
                    WHEN (ar2.reponse_reason = '14') THEN 'Transaction timed out at the ACS'
                    WHEN (ar2.reponse_reason = '15') THEN 'Low confidence'
                    WHEN (ar2.reponse_reason = '16') THEN 'Medium confidence'
                    WHEN (ar2.reponse_reason = '17') THEN 'High confidence'
                    WHEN (ar2.reponse_reason = '18') THEN 'Very High confidence'
                    WHEN (ar2.reponse_reason = '19') THEN 'Exceeds ACS maximum challenges'
                    WHEN (ar2.reponse_reason = '20') THEN 'Non-Payment transaction not supported'
                    WHEN (ar2.reponse_reason = '21') THEN '3RI transaction not supported'
                    WHEN (ar2.reponse_reason = '22') THEN 'ACS technical issue'
                    WHEN (ar2.reponse_reason = '23') THEN 'Decoupled Authentication required by ACS but not requested by 3DS Requestor'
                    WHEN (ar2.reponse_reason = '24') THEN '3DS Requestor Decoupled Max Expiry Time exceeded'
                    WHEN (ar2.reponse_reason = '25') THEN 'Decoupled Authentication was provided insufficient time to authenticate cardholder. ACS will not make attempt'
                    WHEN (ar2.reponse_reason = '26') THEN 'Authentication attempted but not performed by the cardholder'
                    WHEN (((ar2.reponse_reason)::integer >= 27) AND ((ar2.reponse_reason)::integer <= 79)) THEN 'Reserved for EMVCo future use (values invalid until defined by EMVCo)'
                    WHEN (((ar2.reponse_reason)::integer >= 80) AND ((ar2.reponse_reason)::integer <= 99)) THEN 'Reserved for DS use'
                    ELSE 'UNKNOWN'
                END AS reason, --missing data for the threeds schema
                CASE
                    WHEN tvt.eci is not null                            THEN tvt.eci::text  --new change
                    WHEN ("left"(par.request_threeds_version, 1) = '2') THEN replace(COALESCE(r.eci, ar2.eci), '0', '')
                    WHEN ("left"(par.request_threeds_version, 1) = '1') THEN replace(pvr2.eci, '0', '')
                    ELSE replace((mav.eci)::text, '0', '')
                END AS eci, --VV
                CASE
                    WHEN tar.acs_url is not null THEN  true --new change
                    WHEN (ar2.status = 'C') THEN true
                    ELSE false
                END AS was_challenge_requested, --VV
                CASE
                    WHEN tar.errors is not null  and (tar.errors:status)!='SUCCESS' then tar.errors --new change
                    WHEN (cpr2.errors = '[]') THEN NULL
                    WHEN (cpr2.errors IS NOT NULL) THEN cpr2.errors
                    ELSE COALESCE(r.errors, ar2.errors)
                END AS validation_errors, --VV
            parpr.proc_request_id, --VV
            par.auth_request_id, --VV
            part.threeds_id, --VV
            par.num_threeds_requests  AS num_threeds_requests, 
            par.request_data,
            par.payment_attempt_reference_id
           FROM (SELECT par.payment_id
                      ,par.payment_attempt_reference_id
                     , par.request_data:processorId::text AS request_processor_id
                     , par.request_data:processorName::text AS request_processor__name
                     , par.created_at, auth_request_id, request_data
                     , par.request_data:threeDsVersion::text AS request_threeds_version
                     , coalesce(par.request_data:initialRiskChecks.action.name!='threeds',par.request_data:skip3ds::text ) AS request_skip_threeds
                     , count(PAYMENT_ATTEMPT_REFERENCE_ID) over(partition by PAYMENT_ATTEMPT_REFERENCE_ID) AS  num_threeds_requests
                FROM public.payments_auth_requests as par
                WHERE  par.created_at >{today_utc} 
                       AND (is_null_value(request_data:skip3ds) = 'false'
                            OR is_null_value(request_data:initialRiskChecks) = 'false')) par                   
             LEFT JOIN payments_auth_request_mpi_enrollments parme ON parme.auth_request_id = par.auth_request_id
             LEFT JOIN mpi_enrollments me ON me.id = parme.mpi_enrollments_id
             LEFT JOIN payments_auth_request_mpi_authentication_validations parmav ON parmav.auth_request_id = par.auth_request_id
             LEFT JOIN mpi_authentication_validation mav ON mav.id = parmav.mpi_authentication_validation_id
             LEFT JOIN payments_auth_request_proc_requests parpr ON parpr.auth_request_id = par.auth_request_id
             LEFT JOIN payment_auth_request_threeds part ON part.auth_request_id = par.auth_request_id
             LEFT JOIN threeds.initialized_threeds it ON it.id = part.threeds_id   --new change
             LEFT JOIN threeds.authorization_result tar on it.id = tar.threeds_id  --new change
             LEFT JOIN threeds.challenge tc on it.id = tc.threeds_id               --new change
             LEFT JOIN threeds.verified_threeds tvt on it.id = tvt.threeds_id      --new change
             LEFT JOIN threeds_svc.threeds t ON t.id = part.threeds_id
             LEFT JOIN threeds_svc.create_pareq_requests cpr ON t.id = cpr.threeds_id
             LEFT JOIN threeds_svc.create_pareq_responses cpr2 ON cpr.id = cpr2.create_pareq_request_id
             LEFT JOIN threeds_svc.pares_validation_requests pvr ON t.id = pvr.threeds_id
             LEFT JOIN threeds_svc.pares_validation_responses pvr2 ON pvr.id = pvr2.pares_validation_requests_id
             LEFT JOIN threeds_svc.threeds_versioning_requests tvr ON tvr.threeds_id = t.id
             LEFT JOIN threeds_svc.versioning_requests vr ON tvr.version_request_id = vr.id
             LEFT JOIN threeds_svc.versioning_responses vr2 ON vr.id = vr2.versioning_requests_id
             LEFT JOIN threeds_svc.authentication_requests ar ON ar.threeds_id = t.id
             LEFT JOIN LATERAL
                (SELECT raw_response:acsURL::text AS response_acs_url, status
                      , raw_response:authenticationResponse.transStatusReason::text AS reponse_reason
                      , eci, errors, authentication_requests_id
                    FROM threeds_svc.authentication_responses aure
                    WHERE aure.authentication_requests_id = ar.id
            ) ar2 ON 1=1
             LEFT JOIN threeds_svc.results r ON r.threeds_id = t.id
             LEFT JOIN max_auth_request_id mari ON mari.auth_request_id = par.auth_request_id
          WHERE (mari.created_at = part.created_at or mari.auth_request_id is null) 
        )
 SELECT raw_data.auth_request_id,
    raw_data.payment_attempt_reference_id,
    raw_data.payment_id,
    raw_data.total_amount,
    raw_data.currency,
    raw_data.card_scheme,
        CASE
            WHEN raw_data.processor__name is not null then raw_data.processor__name
            WHEN (raw_data.processor_id = '3') THEN 'ecp'
            WHEN (raw_data.processor_id = '5') THEN 'paysafe'
            WHEN (raw_data.processor_id = '7') THEN 'worldline'
            WHEN (raw_data.processor_id = '8') THEN 'safecharge'
            ELSE raw_data.processor_id
        END AS processor,
    raw_data.threeds_provider,
    raw_data.threeds_issuer,
    raw_data.mid,
    raw_data.credit_card_id,
    raw_data.auth_request_created_at,
        CASE
            WHEN ((raw_data.request_data:skip3ds::text) = 'true') THEN NULL::text
            ELSE raw_data.flow
        END AS flow,
    raw_data.is_enrolled,
    raw_data.enrollment_result,
    raw_data.enrollment_errors,
    raw_data.threeds_validation_result,
    raw_data.reason,
        CASE
            WHEN (raw_data.eci IS NOT NULL) THEN concat('0', raw_data.eci)
            ELSE NULL::text
        END AS eci,
    raw_data.was_challenge_requested,
        CASE
            WHEN (raw_data.num_threeds_requests > 1) THEN true
            ELSE false
        END AS was_cascading_performed,
        CASE
            WHEN ((raw_data.threeds_validation_result IS NULL) AND (raw_data.proc_request_id IS NOT NULL)) THEN true
            ELSE false
        END AS was_routed_to_non_3ds,
    raw_data.validation_errors,
    raw_data.proc_request_id,
    raw_data.threeds_id,
    raw_data.request_data
   FROM raw_data
        
            """
    )
    python_task >> snowflake_task_delete >> snowflake_task_insert
