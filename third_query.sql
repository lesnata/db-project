-- What is the daily 7 day rolling total booking amount for March 2023?
create table third_query as
SELECT
  booking_date,
  sum(amount) over (order by booking_date rows between 6 preceding and current row) as rolling_total
FROM (
  SELECT
  	DATE_TRUNC('day', booking_start_time) AS booking_date,
  	SUM(booking_id) AS amount
  FROM booking
  WHERE booking_start_time >= '2023-03-01' AND booking_start_time < '2023-04-01'
    AND created_at is not null
    AND is_demo is false
  GROUP BY booking_date
) AS daily_totals;

-- Business question: should we exclude users.demo_user is true?