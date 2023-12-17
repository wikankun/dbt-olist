{{ 
    config(
        materialized='table'
    ) 
}}

WITH date_spine AS (

  {{
    dbt_utils.date_spine(
      start_date="to_date('01/01/2000', 'mm/dd/yyyy')",
      datepart="day",
      end_date="to_date('01/01/2040', 'mm/dd/yyyy')"
    )
  }}

), calculated as (

  SELECT
    TO_CHAR(date_day, 'yyyymmdd')::INT AS date_dim_id,
    date_day AS date_actual,
    EXTRACT(EPOCH FROM date_day) AS epoch,
    TO_CHAR(date_day, 'TMDay') AS day_name,
    EXTRACT(ISODOW FROM date_day) AS day_of_week,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    date_day - DATE_TRUNC('quarter', date_day)::DATE AS day_of_quarter,
    EXTRACT(DOY FROM date_day) AS day_of_year,
    TO_CHAR(date_day, 'W')::INT AS week_of_month,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    EXTRACT(MONTH FROM date_day) AS month_actual,
    TO_CHAR(date_day, 'TMMonth') AS month_name,
    EXTRACT(QUARTER FROM date_day) AS quarter_actual,
    CASE
        WHEN EXTRACT(QUARTER FROM date_day) = 1 THEN 'First'
        WHEN EXTRACT(QUARTER FROM date_day) = 2 THEN 'Second'
        WHEN EXTRACT(QUARTER FROM date_day) = 3 THEN 'Third'
        WHEN EXTRACT(QUARTER FROM date_day) = 4 THEN 'Fourth'
        END AS quarter_name,
    EXTRACT(YEAR FROM date_day) AS year_actual,
    date_day::DATE + (1 - EXTRACT(ISODOW FROM date_day))::INT AS first_day_of_week,
    date_day::DATE + (7 - EXTRACT(ISODOW FROM date_day))::INT AS last_day_of_week,
    date_day::DATE + (1 - EXTRACT(DAY FROM date_day))::INT AS first_day_of_month,
    (DATE_TRUNC('MONTH', date_day) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
    DATE_TRUNC('quarter', date_day)::DATE AS first_day_of_quarter,
    (DATE_TRUNC('quarter', date_day) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
    TO_DATE(EXTRACT(YEAR FROM date_day) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
    TO_DATE(EXTRACT(YEAR FROM date_day) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
    TO_CHAR(date_day, 'mmyyyy') AS mmyyyy,
    TO_CHAR(date_day, 'mmddyyyy') AS mmddyyyy,
    CASE
        WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN TRUE
        ELSE FALSE
        END AS is_weekend
  FROM date_spine

)

SELECT
  *
FROM calculated
