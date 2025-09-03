-- Identify the top 3 days with the largest spike in appointments compared to the previous day
-- (by clinic and overall).

--Overall
WITH daily_totals AS (
  SELECT
    appointment_date,
    SUM(appointment_count) AS total_appointments
  FROM clinic_daily_counts
  GROUP BY appointment_date
),
with_lag AS (
  SELECT
    appointment_date,
    total_appointments,
    LAG(appointment_date) OVER (ORDER BY appointment_date) AS prev_date,
    LAG(total_appointments) OVER (ORDER BY appointment_date) AS prev_total
  FROM daily_totals
)
SELECT
  appointment_date,
  prev_date,
  total_appointments,
  prev_total,
  (total_appointments - prev_total) AS spike
FROM with_lag
WHERE prev_total IS NOT NULL
ORDER BY spike DESC
LIMIT 3;


--By clinic
WITH per_clinic_daily AS (
  SELECT
    clinic_id,
    appointment_date,
    SUM(appointment_count) AS total_appointments
  FROM clinic_daily_counts
  GROUP BY clinic_id, appointment_date
),
with_lag AS (
  SELECT
    clinic_id,
    appointment_date,
    total_appointments,
    LAG(appointment_date) OVER (
      PARTITION BY clinic_id
      ORDER BY appointment_date
    ) AS prev_date,
    LAG(total_appointments) OVER (
      PARTITION BY clinic_id
      ORDER BY appointment_date
    ) AS prev_total
  FROM per_clinic_daily
),
scored AS (
  SELECT
    clinic_id,
    appointment_date,
    prev_date,
    total_appointments,
    prev_total,
    (total_appointments - prev_total) AS spike
  FROM with_lag
  WHERE prev_total IS NOT NULL
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY clinic_id
      ORDER BY spike DESC, appointment_date
    ) AS rn
  FROM scored
)
SELECT
  clinic_id,
  appointment_date,
  prev_date,
  total_appointments,
  prev_total,
  spike
FROM ranked
WHERE rn <= 3
ORDER BY clinic_id, spike DESC;

