-- How many users need more than 30 days to make their first booking, and from which company are those users ?
create table second_query as
    SELECT
        c.company_name,
        COUNT(*) AS num_users
    FROM users u
    LEFT JOIN (
      SELECT
        b.user_id,
        MIN(b.booking_start_time) AS first_booking_time
      FROM booking b
      WHERE b.created_at is not null
        AND b.is_demo is false
      GROUP BY b.user_id
    ) AS fb
    ON u.rn = fb.user_id
    join company as c on u.company_id = c.company_id
    WHERE fb.first_booking_time IS NOT NULL
    AND fb.first_booking_time - u.created_at > INTERVAL '30 days'
    AND u.demo_user is false
    GROUP BY 1;

-- Business questions: should we filter out those values?