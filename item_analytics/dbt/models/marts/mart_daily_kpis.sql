{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- Daily KPI aggregations for ITSM performance monitoring
WITH daily_metrics AS (
  SELECT 
    created_date,
    created_year,
    created_month,
    created_week,
    weekend_flag,
    
    -- Volume metrics
    COUNT(*) AS total_incidents,
    COUNT(CASE WHEN current_state = 'Closed' THEN 1 END) AS incidents_closed,
    COUNT(CASE WHEN current_state IN ('New', 'In Progress', 'On Hold') THEN 1 END) AS incidents_open,
    
    -- Priority breakdown
    COUNT(CASE WHEN priority_level = 'Critical' THEN 1 END) AS critical_incidents,
    COUNT(CASE WHEN priority_level = 'High' THEN 1 END) AS high_incidents,
    COUNT(CASE WHEN priority_level = 'Moderate' THEN 1 END) AS moderate_incidents,
    COUNT(CASE WHEN priority_level = 'Low' THEN 1 END) AS low_incidents,
    
    -- Category breakdown
    COUNT(CASE WHEN category = 'Software' THEN 1 END) AS software_incidents,
    COUNT(CASE WHEN category = 'Hardware-Infrastructure' THEN 1 END) AS hardware_incidents,
    COUNT(CASE WHEN category = 'Workstation' THEN 1 END) AS workstation_incidents,
    COUNT(CASE WHEN category = 'Network' THEN 1 END) AS network_incidents,
    
    -- Resolution time metrics (for closed incidents only)
    AVG(CASE WHEN current_state = 'Closed' THEN resolution_time_hours END) AS avg_resolution_time_hours,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN current_state = 'Closed' THEN resolution_time_hours END) AS median_resolution_time_hours,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY CASE WHEN current_state = 'Closed' THEN resolution_time_hours END) AS p90_resolution_time_hours,
    MIN(CASE WHEN current_state = 'Closed' THEN resolution_time_hours END) AS min_resolution_time_hours,
    MAX(CASE WHEN current_state = 'Closed' THEN resolution_time_hours END) AS max_resolution_time_hours,
    
    -- SLA metrics
    COUNT(CASE WHEN sla_status = 'Met' THEN 1 END) AS sla_met_count,
    COUNT(CASE WHEN sla_status = 'Missed' THEN 1 END) AS sla_missed_count,
    COUNT(CASE WHEN sla_status != 'Unknown' THEN 1 END) AS sla_applicable_count,
    
    -- First Contact Resolution
    COUNT(CASE WHEN fcr_flag = 'FCR' THEN 1 END) AS fcr_count,
    
    -- Business hours vs off-hours
    COUNT(CASE WHEN created_during_business_hours = 'Business Hours' THEN 1 END) AS business_hours_incidents,
    COUNT(CASE WHEN created_during_business_hours = 'Off Hours' THEN 1 END) AS off_hours_incidents,
    
    -- Same day resolution
    COUNT(CASE WHEN resolution_timeframe = 'Same Day' THEN 1 END) AS same_day_resolutions
    
  FROM {{ ref('mart_incident_summary') }}
  GROUP BY 
    created_date,
    created_year,
    created_month,
    created_week,
    weekend_flag
),

calculated_kpis AS (
  SELECT 
    *,
    
    -- Calculate percentage metrics
    CASE 
      WHEN total_incidents > 0 
      THEN ROUND((incidents_closed::DECIMAL / total_incidents) * 100, 2) 
      ELSE 0 
    END AS closure_rate_pct,
    
    CASE 
      WHEN sla_applicable_count > 0 
      THEN ROUND((sla_met_count::DECIMAL / sla_applicable_count) * 100, 2) 
      ELSE 0 
    END AS sla_compliance_pct,
    
    CASE 
      WHEN incidents_closed > 0 
      THEN ROUND((fcr_count::DECIMAL / incidents_closed) * 100, 2) 
      ELSE 0 
    END AS fcr_rate_pct,
    
    CASE 
      WHEN incidents_closed > 0 
      THEN ROUND((same_day_resolutions::DECIMAL / incidents_closed) * 100, 2) 
      ELSE 0 
    END AS same_day_resolution_pct,
    
    CASE 
      WHEN total_incidents > 0 
      THEN ROUND((critical_incidents::DECIMAL / total_incidents) * 100, 2) 
      ELSE 0 
    END AS critical_incident_pct,
    
    CASE 
      WHEN total_incidents > 0 
      THEN ROUND(((critical_incidents + high_incidents)::DECIMAL / total_incidents) * 100, 2) 
      ELSE 0 
    END AS high_priority_incident_pct
    
  FROM daily_metrics
)

SELECT 
  created_date,
  created_year,
  created_month,
  created_week,
  weekend_flag,
  
  -- Volume
  total_incidents,
  incidents_closed,
  incidents_open,
  
  -- Priority distribution
  critical_incidents,
  high_incidents,
  moderate_incidents,
  low_incidents,
  
  -- Category distribution  
  software_incidents,
  hardware_incidents,
  workstation_incidents,
  network_incidents,
  
  -- Resolution time statistics
  avg_resolution_time_hours,
  median_resolution_time_hours,
  p90_resolution_time_hours,
  min_resolution_time_hours,
  max_resolution_time_hours,
  
  -- SLA metrics
  sla_met_count,
  sla_missed_count,
  sla_applicable_count,
  sla_compliance_pct,
  
  -- Other KPIs
  fcr_count,
  fcr_rate_pct,
  same_day_resolutions,
  same_day_resolution_pct,
  business_hours_incidents,
  off_hours_incidents,
  closure_rate_pct,
  critical_incident_pct,
  high_priority_incident_pct,
  
  -- Metadata
  CURRENT_TIMESTAMP AS dbt_updated_at
  
FROM calculated_kpis
ORDER BY created_date DESC