create table booking as
select
    user_id,
    booking_id,
    case when trim(created_at, '"') = 'NULL' then null
        else trim(created_at, '"')::timestamp end as created_at,
    TRIM(BOTH ' ' FROM CAST(status AS VARCHAR)) as status,
    TRIM(BOTH ' ' FROM CAST(checkin_status AS VARCHAR)) as checkin_status,
    case when trim(booking_start_time, '"') = 'NULL' then null
        else trim(booking_start_time, '"')::timestamp end as booking_start_time,
    case when trim(booking_end_time, '"') = 'NULL' then null
            else trim(booking_end_time, '"')::timestamp end as booking_end_time,
    trim(is_demo, '"')::boolean as is_demo,
    CURRENT_TIMESTAMP as current_timestamp
from booking_raw
where created_at not like '%ABC%'
and created_at not like '%XYZ%';

-- Business ETL questions:
-- AND created_at is not null
-- AND is_demo is false