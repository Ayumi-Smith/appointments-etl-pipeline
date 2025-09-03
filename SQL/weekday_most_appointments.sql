--Which day of the week (Monday–Sunday) tends to have the most appointments overall?
--Calculating average number of appointments for weekday within all clinics

WITH daily_totals AS (
    SELECT
        appointment_date,
        SUM(appointment_count) AS total_per_day
    FROM clinic_daily_counts
    GROUP BY appointment_date
)
SELECT
    CASE strftime('%w', appointment_date)
        WHEN '0' THEN 'Sunday'
        WHEN '1' THEN 'Monday'
        WHEN '2' THEN 'Tuesday'
        WHEN '3' THEN 'Wednesday'
        WHEN '4' THEN 'Thursday'
        WHEN '5' THEN 'Friday'
        WHEN '6' THEN 'Saturday'
    END AS weekday_name,
    AVG(total_per_day) AS avg_appointments
FROM daily_totals
GROUP BY strftime('%w', appointment_date)
ORDER BY avg_appointments DESC
LIMIT 1;
