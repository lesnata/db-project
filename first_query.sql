-- What is the monthly count of unique Users (headcount) who have made a booking for the last 6 months?
create table first_query as
    SELECT date_trunc('month', b.created_at) AS month,
    COUNT(DISTINCT user_id) AS unique_users
FROM booking as b
join users as u on b.user_id = u.rn
WHERE booking_start_time > NOW() - INTERVAL '6 months'
and u.demo_user is false
and b.created_at is not null
and b.is_demo is false
GROUP BY date_trunc('month', b.created_at);
