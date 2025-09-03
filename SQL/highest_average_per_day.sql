--Which clinic had the highest average number of appointments per day across the dataset?

SELECT
    clinic_id,
    AVG(appointment_count) AS avg_per_day
FROM clinic_daily_counts
GROUP BY clinic_id
ORDER BY avg_per_day DESC
LIMIT 1;