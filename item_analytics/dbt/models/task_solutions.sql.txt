--Calculate the average resolution time per Category and Priority
SELECT 
    inc_category,
    inc_priority,
    AVG(inc_resolved_at - inc_sys_created_on) AS avg_resolution_time
FROM incident_data
GROUP BY 
    inc_category,
    inc_priority;

--Calculate ticket closure rate per Assigned Group
SELECT 
    inc_assignment_group,
    COUNT(*) AS total_tickets,
    COUNT(*) FILTER (WHERE LOWER(inc_state) = 'closed') AS closed_tickets,
    ROUND(
        COUNT(*) FILTER (WHERE LOWER(inc_state) = 'closed')::NUMERIC 
        / COUNT()  100, 
        2
    ) AS closure_rate_percent
FROM incident_data
GROUP BY inc_assignment_group;

--Create a Monthly Ticket Summary table aggregating the number of tickets, average resolution time, and closure rate per month
CREATE TABLE monthly_ticket_summary AS
SELECT 
    TO_CHAR(inc_sys_created_on, 'YYYY-MM') AS ticket_month,
    COUNT(*) AS total_tickets,
    ROUND(AVG(EXTRACT(EPOCH FROM (inc_resolved_at - inc_sys_created_on)) / 3600), 2) AS avg_resolution_time_hours,
    ROUND(
        SUM(CASE WHEN LOWER(inc_state) = 'closed' THEN 1 ELSE 0 END)::NUMERIC  100 / COUNT(),
        2
    ) AS closure_rate_percent
FROM 
    incident_details
GROUP BY 
    TO_CHAR(inc_sys_created_on, 'YYYY-MM')
ORDER BY 
    ticket_month;