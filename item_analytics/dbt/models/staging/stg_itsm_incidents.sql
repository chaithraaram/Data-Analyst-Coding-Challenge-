{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH source_data AS (
  SELECT 
    inc_business_service,
    inc_category,
    inc_number,
    inc_priority,
    inc_sla_due,
    inc_sys_created_on,
    inc_resolved_at,
    inc_assigned_to,
    inc_state,
    inc_cmdb_ci,
    inc_caller_id,
    inc_short_description,
    inc_assignment_group,
    inc_close_code,
    inc_close_notes,
    resolution_time_hours
  FROM {{ source('raw', 'itsm_raw_tickets') }}
)

SELECT 
  -- Incident identification
  inc_number AS incident_number,
  inc_business_service AS business_service,
  inc_category AS category,
  inc_state AS current_state,
  
  -- Priority and SLA information
  CASE 
    WHEN inc_priority = '1 - Critical' THEN 'Critical'
    WHEN inc_priority = '2 - High' THEN 'High'
    WHEN inc_priority = '3 - Moderate' THEN 'Moderate'
    WHEN inc_priority = '4 - Low' THEN 'Low'
    ELSE 'Unknown'
  END AS priority_level,
  
  CASE 
    WHEN inc_priority = '1 - Critical' THEN 1
    WHEN inc_priority = '2 - High' THEN 2
    WHEN inc_priority = '3 - Moderate' THEN 3
    WHEN inc_priority = '4 - Low' THEN 4
    ELSE 999
  END AS priority_rank,
  
  inc_sla_due AS sla_due,
  
  -- Timestamps
  inc_sys_created_on AS created_at,
  inc_resolved_at AS resolved_at,
  
  -- Assignment information
  inc_assigned_to AS assigned_to,
  inc_assignment_group AS assignment_group,
  inc_caller_id AS caller_id,
  
  -- Configuration management
  inc_cmdb_ci AS configuration_item,
  
  -- Description and resolution
  inc_short_description AS short_description,
  inc_close_code AS close_code,
  inc_close_notes AS close_notes,
  
  -- Calculated metrics
  resolution_time_hours,
  
  -- Business hours calculation (assuming 8 hours per day, 5 days per week)
  CASE 
    WHEN resolution_time_hours IS NOT NULL 
    THEN resolution_time_hours / 8.0  -- Convert to business days
    ELSE NULL 
  END AS resolution_time_business_days,
  
  -- SLA compliance flags
  CASE 
    WHEN inc_priority = '1 - Critical' AND resolution_time_hours <= 4 THEN 'Met'
    WHEN inc_priority = '2 - High' AND resolution_time_hours <= 24 THEN 'Met'
    WHEN inc_priority = '3 - Moderate' AND resolution_time_hours <= 72 THEN 'Met'
    WHEN inc_priority = '4 - Low' AND resolution_time_hours <= 168 THEN 'Met'
    WHEN resolution_time_hours IS NULL THEN 'Unknown'
    ELSE 'Missed'
  END AS sla_status,
  
  -- Date dimensions for easier aggregation
  DATE(inc_sys_created_on) AS created_date,
  EXTRACT(YEAR FROM inc_sys_created_on) AS created_year,
  EXTRACT(MONTH FROM inc_sys_created_on) AS created_month,
  EXTRACT(WEEK FROM inc_sys_created_on) AS created_week,
  EXTRACT(DOW FROM inc_sys_created_on) AS created_day_of_week,
  EXTRACT(HOUR FROM inc_sys_created_on) AS created_hour,
  
  -- Current timestamp for tracking data freshness
  CURRENT_TIMESTAMP AS dbt_updated_at

FROM source_data

-- Data quality filters
WHERE inc_number IS NOT NULL 
  AND inc_sys_created_on IS NOT NULL