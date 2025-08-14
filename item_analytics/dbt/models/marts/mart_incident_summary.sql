{{
  config(
    materialized='table',
    schema='marts'
  )
}}

WITH incident_metrics AS (
  SELECT 
    incident_number,
    category,
    priority_level,
    priority_rank,
    current_state,
    assignment_group,
    assigned_to,
    business_service,
    close_code,
    created_at,
    resolved_at,
    resolution_time_hours,
    resolution_time_business_days,
    sla_status,
    created_date,
    created_year,
    created_month,
    created_week,
    created_day_of_week,
    created_hour,
    
    -- Calculate if incident was resolved same day
    CASE 
      WHEN DATE(created_at) = DATE(resolved_at) THEN 'Same Day'
      WHEN resolved_at IS NULL THEN 'Unresolved'
      ELSE 'Multi-Day'
    END AS resolution_timeframe,
    
    -- Categorize resolution time
    CASE 
      WHEN resolution_time_hours IS NULL THEN 'Unresolved'
      WHEN resolution_time_hours <= 1 THEN '≤ 1 hour'
      WHEN resolution_time_hours <= 4 THEN '1-4 hours'
      WHEN resolution_time_hours <= 24 THEN '4-24 hours'
      WHEN resolution_time_hours <= 72 THEN '1-3 days'
      WHEN resolution_time_hours <= 168 THEN '3-7 days'
      ELSE '> 1 week'
    END AS resolution_time_bucket,
    
    -- First contact resolution flag
    CASE 
      WHEN resolution_time_hours <= 1 THEN 'FCR'
      ELSE 'Non-FCR'
    END AS fcr_flag,
    
    -- Business hours flag
    CASE 
      WHEN created_hour BETWEEN 9 AND 17 
       AND created_day_of_week BETWEEN 1 AND 5 THEN 'Business Hours'
      ELSE 'Off Hours'
    END AS created_during_business_hours,
    
    -- Age in days for open tickets
    CASE 
      WHEN current_state IN ('New', 'In Progress', 'On Hold') 
      THEN EXTRACT(DAY FROM CURRENT_TIMESTAMP - created_at)
      ELSE NULL 
    END AS days_open
    
  FROM {{ ref('stg_itsm_incidents') }}
),

enriched_incidents AS (
  SELECT 
    *,
    
    -- Aging categories for open tickets
    CASE 
      WHEN days_open IS NULL THEN NULL
      WHEN days_open <= 1 THEN 'New (≤ 1 day)'
      WHEN days_open <= 7 THEN 'Recent (1-7 days)'
      WHEN days_open <= 30 THEN 'Aging (1-4 weeks)'
      ELSE 'Critical Age (> 30 days)'
    END AS aging_bucket,
    
    -- Volume patterns
    CASE 
      WHEN created_hour BETWEEN 6 AND 12 THEN 'Morning'
      WHEN created_hour BETWEEN 12 AND 18 THEN 'Afternoon'
      WHEN created_hour BETWEEN 18 AND 24 THEN 'Evening'
      ELSE 'Night'
    END AS time_of_day_created,
    
    -- Weekend flag
    CASE 
      WHEN created_day_of_week IN (0, 6) THEN 'Weekend'
      ELSE 'Weekday'
    END AS weekend_flag
    
  FROM incident_metrics
)

SELECT * FROM enriched_incidents