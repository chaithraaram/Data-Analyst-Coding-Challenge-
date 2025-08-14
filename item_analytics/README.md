# ITSM Data Analytics Pipeline

## Overview

This project implements a comprehensive data pipeline for IT Service Management (ITSM) analytics using Apache Airflow, DBT, PostgreSQL, and Apache Superset. The pipeline processes ServiceNow incident ticket data to provide insights into service performance, SLA compliance, and operational efficiency.

## Architecture

```
Excel Data Source → Airflow → PostgreSQL → DBT → Superset Dashboard
```

### Components
- **Apache Airflow**: Orchestrates the data pipeline
- **PostgreSQL**: Data warehouse for storing raw and transformed data
- **DBT**: Handles data transformations and modeling
- **Apache Superset**: Provides interactive dashboards and visualizations

## Project Structure

```
itsm-analytics/
├── airflow/
│   └── dags/
│       └── itsm_data_pipeline.py      # Main Airflow DAG
├── dbt/
│   ├── dbt_project.yml                # DBT project configuration
│   ├── profiles.yml                   # Database connection profiles
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_itsm_incidents.sql # Staging model for raw data
│   │   └── marts/
│   │       ├── mart_incident_summary.sql    # Main incident analysis mart
│   │       ├── mart_daily_kpis.sql          # Daily KPI aggregations
│   │       └── mart_group_performance.sql   # Assignment group performance
│   └── sources.yml                    # Source data definitions
├── superset/
│   └── superset_dashboard.json        # Dashboard export
├── data/
│   └── Sample-Data-file-for-Analysis_Jan-25-1.xlsx
└── README.md
```

## Data Model

### Source Data
The pipeline processes ServiceNow incident tickets with the following key fields:
- Incident identification (number, category, priority)
- Timestamps (created, resolved)
- Assignment information (group, individual)
- Resolution details (close code, notes)

### Transformed Data Models

#### Staging Layer (`stg_itsm_incidents`)
- Cleans and standardizes raw data
- Calculates resolution times
- Adds business logic for SLA compliance
- Creates date dimensions for analysis

#### Marts Layer
1. **`mart_incident_summary`**: Comprehensive incident analysis with enriched metrics
2. **`mart_daily_kpis`**: Daily aggregated KPIs for trend analysis
3. **`mart_group_performance`**: Assignment group performance metrics

## Key Metrics & KPIs

### Volume Metrics
- Total incidents by period
- Incident distribution by priority, category, and state
- Open vs. closed incident ratios

### Performance Metrics
- Average, median, and P90 resolution times
- First Contact Resolution (FCR) rates
- SLA compliance percentages
- Same-day resolution rates

### Operational Metrics
- Business hours vs. off-hours incident creation
- Assignment group workload and specialization
- Age distribution of open tickets
- Close code analysis

## Assumptions

### Business Rules
1. **SLA Targets**:
   - Critical: 4 hours
   - High: 24 hours
   - Moderate: 72 hours
   - Low: 168 hours (7 days)

2. **Business Hours**: 9 AM - 5 PM, Monday-Friday

3. **First Contact Resolution**: Incidents resolved within 1 hour

### Data Quality Assumptions
- Incident numbers are unique identifiers
- Creation timestamps are always present
- Resolution times are calculated only for closed incidents
- Negative resolution times are considered data quality issues

## Setup Instructions

### Prerequisites
- Docker and Docker Compose
- Python 3.8+
- PostgreSQL 12+

### 1. Environment Setup

```bash
# Clone the repository
git clone <repository-url>
cd itsm-analytics

# Create Python virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install apache-airflow[postgres]==2.5.0
pip install dbt-postgres==1.4.0
pip install pandas openpyxl sqlalchemy psycopg2-binary
```

### 2. Database Setup

```bash
# Start PostgreSQL with Docker
docker run --name postgres-itsm \
  -e POSTGRES_USER=airflow \
  -e POSTGRES_PASSWORD=airflow \
  -e POSTGRES_DB=airflow \
  -p 5432:5432 \
  -d postgres:13

# Create analytics schema
psql -h localhost -U airflow -d airflow -c "CREATE SCHEMA IF NOT EXISTS analytics;"
```

### 3. Airflow Setup

```bash
# Initialize Airflow database
export AIRFLOW_HOME=~/airflow
airflow db init

# Create admin user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# Start Airflow webserver and scheduler
airflow webserver --port 8080 &
airflow scheduler &
```

### 4. DBT Setup

```bash
# Navigate to DBT project directory
cd dbt/

# Install DBT dependencies
dbt deps

# Test connection
dbt debug

# Run initial transformations
dbt run
dbt test
```

### 5. Data Loading

```bash
# Place the Excel file in the data directory
mkdir -p /opt/airflow/data
cp Sample-Data-file-for-Analysis_Jan-25-1.xlsx /opt/airflow/data/

# Trigger the Airflow DAG
airflow dags trigger itsm_data_pipeline
```

### 6. Superset Setup

```bash
# Start Superset with Docker
docker run -d --name superset \
  -p 8088:8088 \
  apache/superset:latest

# Initialize Superset
docker exec -it superset superset fab create-admin \
  --username admin \
  --firstname Superset \
  --lastname Admin \
  --email admin@superset.com \
  --password admin

docker exec -it superset superset db upgrade
docker exec -it superset superset init

# Import dashboard
docker cp superset_dashboard.json superset:/tmp/
docker exec -it superset superset import_dashboards -p /tmp/superset_dashboard.json
```

## Usage

### Running the Pipeline

1. **Access Airflow UI**: http://localhost:8080
   - Username: admin
   - Password: admin

2. **Trigger the DAG**: 
   - Navigate to DAGs page
   - Find `itsm_data_pipeline`
   - Click "Trigger DAG"

3. **Monitor Progress**: 
   - View task execution in Graph View
   - Check logs for any issues

### Accessing Dashboards

1. **Open Superset**: http://localhost:8088
   - Username: admin
   - Password: admin

2. **View Dashboard**: 
   - Navigate to Dashboards
   - Open "ITSM Service Management Dashboard"

### Query Data Directly

```sql
-- Example queries for analysis

-- Daily incident volume
SELECT created_date, total_incidents, sla_compliance_pct
FROM analytics.mart_daily_kpis
ORDER BY created_date DESC;

-- Top performing assignment groups
SELECT assignment_group, total_assigned_incidents, avg_resolution_time_hours, sla_compliance_pct
FROM analytics.mart_group_performance
ORDER BY sla_compliance_pct DESC, total_assigned_incidents DESC;

-- Open ticket aging
SELECT aging_bucket, COUNT(*) as count
FROM analytics.mart_incident_summary
WHERE current_state IN ('New', 'In Progress', 'On Hold')
GROUP BY aging_bucket;
```

## Monitoring & Maintenance

### Data Quality Checks
- DBT tests validate data integrity
- Airflow DAG includes data quality tasks
- Monitor for resolution time outliers

### Refresh Schedule
- Default: Daily execution at midnight
- Configurable in Airflow DAG
- Manual triggers available for ad-hoc analysis

### Performance Optimization
- Indexes on frequently queried columns
- Materialized tables for marts
- Views for staging models

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Verify PostgreSQL is running
   - Check connection strings in profiles.yml
   - Ensure schemas exist

2. **Data Loading Failures**
   - Confirm Excel file path is correct
   - Check file permissions
   - Verify pandas can read the file

3. **DBT Run Errors**
   - Review model syntax
   - Check source data availability
   - Validate data types

### Logs and Debugging
- Airflow logs: Available in UI under task instances
- DBT logs: `logs/dbt.log` in project directory
- PostgreSQL logs: Docker container logs

## Future Enhancements

### Potential Improvements
1. **Real-time Streaming**: Implement Kafka for real-time data ingestion
2. **Machine Learning**: Predict SLA breaches and resolution times
3. **Alerting**: Set up automated alerts for KPI thresholds
4. **Advanced Analytics**: Customer satisfaction correlation analysis
5. **Data Governance**: Implement data lineage and catalog

### Scalability Considerations
- Partition large tables by date
- Implement incremental data loading
- Consider cloud deployment (AWS, GCP, Azure)
- Add data lake for historical archival

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and test thoroughly
4. Submit a pull request with clear description

## License

This project is licensed under the MIT License - see LICENSE file for details.