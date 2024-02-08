create or replace function sp_get_hash_for_email(email_val character varying, pepper_val character varying) returns character varying
    language plpgsql
as
$$
DECLARE hash_val varchar;
DECLARE attempts integer := 1;

BEGIN

    if email_val is not null and length(email_val)>0 then

        loop
            begin
                select hash into hash_val from email_mapping.emails emails where emails.email = email_val ;

                if hash_val is null then
                    select sha256((email_val || pepper_val)::bytea) into hash_val;

                    INSERT INTO email_mapping.emails(email,pepper,hash) VALUES (email_val,pepper_val,hash_val);

                    select sha256((  encode(sha256(salt::text::bytea),'escape') || email::varchar || pepper::varchar )::bytea) into hash_val from email_mapping.emails emails where emails.email = email_val ;

                    update email_mapping.emails
                    set hash = hash_val
                    where email = email_val;
                end if;
                return hash_val;
            exception
                when unique_violation then
                    if attempts>3 then
                        raise exception 'unique violation, email %s already exists', email_val;
                    end if;
                    attempts = attempts+1;
                    PERFORM pg_sleep(1);
            end;

        end loop;
    end if;

    return hash_val;

END;
$$;