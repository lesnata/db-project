CREATE table users as select
           rn,
           created_at,
           company_id,
           TRIM(BOTH ' ' FROM CAST(status AS VARCHAR)) as status,
           TRIM(demo_user, '"')::boolean as demo_user,
           CURRENT_TIMESTAMP as current_timestamp
FROM users_raw;

-- Business ETL questions:
--  u.demo_user is false