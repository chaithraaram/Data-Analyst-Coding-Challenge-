{{
  config(
    materialized='table',
    schema='marts'
  )
}}

-- Assignment group performance analysis
WITH group_metrics AS (
  SELECT 
    assignment_group,
    
    -- Volume metrics
    COUNT(*) AS total_assigned_incidents,
    COUNT(CASE WHEN current_state = 'Closed' THEN 1 END) AS incidents_resolved,
    COUNT(CASE WHEN current_state IN ('New', 'In Progress', 'On Hold') THEN 1 END) AS incidents_pending,
    
    -- Priority handling
    COUNT(CASE WHEN priority_level = 'Critical' THEN 1 END) AS critical_incidents,
    COUNT(CASE WHEN priority_level = 'High' THEN 1 END) AS high_incidents,
    
    -- Category specialization
    COUNT(CASE WHEN category = 'Software' THEN 1 END) AS software_incidents,
    COUNT(CASE WHEN category = 'Hardware-Infrastructure' THEN 1 END) AS hardware_incidents,
    COUNT(CASE WHEN category = 'Workstation' THEN 1 END) AS workstation_incidents,
    COUNT(CASE WHEN category = 'Network' THEN 1 END) AS network_incidents,
    
    -- Performance metrics (resolved incidents only)
    AVG(CASE WHEN current_state = 'Closed' THEN resolution_time_hours END) AS avg_resolution_time_hours,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN current_state = 'Closed' THEN resolution_time_hours END) AS median_resolution_time_hours,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY CASE WHEN current_state = 'Closed' THEN resolution_time_hours END) AS p90_resolution_time_hours,
    
    -- SLA performance
    COUNT(CASE WHEN sla_status = 'Met' THEN 1 END) AS sla_met,
    COUNT(CASE WHEN sla_status = 'Missed' THEN 1 END) AS sla_missed,
    COUNT(CASE WHEN sla_status != 'Unknown' THEN 1 END) AS sla_applicable,
    
    -- First Contact Resolution
    COUNT(CASE WHEN fcr_flag = 'FCR' AND current_state = 'Closed' THEN 1 END) AS fcr_resolutions,
    
    -- Close code distribution (top patterns)
    COUNT(CASE WHEN close_code = 'Other not on list' THEN 1 END) AS other_close_code,
    COUNT(CASE WHEN close_code = 'User Education and Training' THEN 1 END) AS training_close_code,
    COUNT(CASE WHEN close_code = 'Configuration Change' THEN 1 END) AS config_change_close_code,
    COUNT(CASE WHEN close_code = 'No Fault Found' THEN 1 END) AS no_fault_close_code,
    
    -- Aging of open tickets
    AVG(CASE WHEN current_state IN ('New', 'In Progress', 'On Hold') THEN days_open END) AS avg_open_age_days,
    MAX(CASE WHEN current_state IN ('New', 'In Progress', 'On Hold') THEN days_open END) AS max_open_age_days,
    
    -- Date range for the analysis
    MIN(created_date) AS first_incident_date,
    MAX(created_date) AS last_incident_date
    
  FROM {{ ref('mart_incident_summary') }}
  WHERE assignment_group IS NOT NULL
  GROUP BY assignment_group
),

group_performance AS (
  SELECT 
    *,
    
    -- Calculate performance ratios
    CASE 
      WHEN total_assigned_incidents > 0 
      THEN ROUND((incidents_resolved::DECIMAL / total_assigned_incidents) * 100, 2) 
      ELSE 0 
    END AS resolution_rate_pct,
    
    CASE 
      WHEN sla_applicable > 0 
      THEN ROUND((sla_met::DECIMAL / sla_applicable) * 100, 2) 
      ELSE 0 
    END AS sla_compliance_pct,
    
    CASE 
      WHEN incidents_resolved > 0 
      THEN ROUND((fcr_resolutions::DECIMAL / incidents_resolved) * 100, 2) 
      ELSE 0 
    END AS fcr_rate_pct,
    
    -- Workload indicators
    CASE 
      WHEN total_assigned_incidents > 0 
      THEN ROUND(((critical_incidents + high_incidents)::DECIMAL / total_assigned_incidents) * 100, 2) 
      ELSE 0 
    END AS high_priority_workload_pct,
    
    -- Specialization score (dominant category)
    GREATEST(software_incidents, hardware_incidents, workstation_incidents, network_incidents) AS dominant_category_count,
    
    CASE 
      WHEN software_incidents = GREATEST(software_incidents, hardware_incidents, workstation_incidents, network_incidents) THEN 'Software'
      WHEN hardware_incidents = GREATEST(software_incidents, hardware_incidents, workstation_incidents, network_incidents) THEN 'Hardware'
      WHEN workstation_incidents = GREATEST(software_incidents, hardware_incidents, workstation_incidents, network_incidents) THEN 'Workstation'
      WHEN network_incidents = GREATEST(software_incidents, hardware_incidents, workstation_incidents, network_incidents) THEN 'Network'
      ELSE 'Mixed'
    END AS primary_specialization
    
  FROM group_metrics
),

ranked_groups AS (
  SELECT 
    *,
    
    -- Performance rankings
    ROW_NUMBER() OVER (ORDER BY total_assigned_incidents DESC) AS volume_rank,
    ROW_NUMBER() OVER (ORDER BY avg_resolution_time_hours ASC) AS speed_rank,
    ROW_NUMBER() OVER (ORDER BY sla_compliance_pct DESC) AS sla_rank,
    ROW_NUMBER() OVER (ORDER BY fcr_rate_pct DESC) AS fcr_rank,
    
    -- Efficiency score (composite metric)
    CASE 
      WHEN sla_compliance_pct >= 90 AND fcr_rate_pct >= 20 AND avg_resolution_time_hours <= 48 THEN 'High Performer'
      WHEN sla_compliance_pct >= 80 AND avg_resolution_time_hours <= 72 THEN 'Good Performer'
      WHEN sla_compliance_pct >= 70 THEN 'Average Performer'
      ELSE 'Needs Improvement'
    END AS performance_tier
    
  FROM group_performance
)

SELECT 
  assignment_group,
  
  -- Volume and workload
  total_assigned_incidents,
  incidents_resolved,
  incidents_pending,
  volume_rank,
  
  -- Priority distribution
  critical_incidents,
  high_incidents,
  high_priority_workload_pct,
  
  -- Specialization
  primary_specialization,
  software_incidents,
  hardware_incidents,
  workstation_incidents,
  network_incidents,
  
  -- Performance metrics
  avg_resolution_time_hours,
  median_resolution_time_hours,
  p90_resolution_time_hours,
  speed_rank,
  
  -- Quality metrics
  resolution_rate_pct,
  sla_compliance_pct,
  sla_rank,
  fcr_rate_pct,
  fcr_rank,
  
  -- Open ticket management
  avg_open_age_days,
  max_open_age_days,
  
  -- Overall assessment
  performance_tier,
  
  -- Close code patterns
  other_close_code,
  training_close_code,
  config_change_close_code,
  no_fault_close_code,
  
  -- Metadata
  first_incident_date,
  last_incident_date,
  CURRENT_TIMESTAMP AS dbt_updated_at
  
FROM ranked_groups
WHERE total_assigned_incidents >= 5  -- Filter for groups with meaningful volume
ORDER BY total_assigned_incidents DESC