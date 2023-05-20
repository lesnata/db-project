create table company AS
    SELECT
           company_id,
           status,
           created_at,
           TRIM(BOTH ' ' FROM CAST(company_name AS VARCHAR)) as company_name,
           CURRENT_TIMESTAMP as current_timestamp
    FROM company_raw;