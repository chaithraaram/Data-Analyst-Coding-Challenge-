"""
ITSM Data Pipeline DAG
=====================

This DAG orchestrates the ITSM data pipeline including:
1. Data extraction from source Excel file
2. Data loading to PostgreSQL staging
3. DBT transformations
4. Data quality checks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'data_analyst',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'itsm_data_pipeline',
    default_args=default_args,
    description='ITSM ServiceNow ticket data processing pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['itsm', 'servicenow', 'analytics'],
)

def extract_itsm_data(**context):
    """
    Extract data from Excel file and perform initial data quality checks
    """
    try:
        # Read Excel file
        df = pd.read_excel('/opt/airflow/data/Sample-Data-file-for-Analysis_Jan-25-1.xlsx', 
                          sheet_name='Raw Data')
        
        # Log basic statistics
        logging.info(f"Extracted {len(df)} records")
        logging.info(f"Columns: {list(df.columns)}")
        
        # Basic data quality checks
        missing_data = df.isnull().sum()
        logging.info(f"Missing data per column: {missing_data[missing_data > 0].to_dict()}")
        
        # Calculate resolution time
        df['resolution_time_hours'] = (
            pd.to_datetime(df['inc_resolved_at']) - 
            pd.to_datetime(df['inc_sys_created_on'])
        ).dt.total_seconds() / 3600
        
        # Save to staging area
        df.to_csv('/tmp/itsm_staging.csv', index=False)
        logging.info("Data extracted and saved to staging area")
        
    except Exception as e:
        logging.error(f"Error in data extraction: {str(e)}")
        raise

def load_to_postgres(**context):
    """
    Load extracted data to PostgreSQL staging table
    """
    try:
        # Read staging data
        df = pd.read_csv('/tmp/itsm_staging.csv')
        
        # Connect to PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = postgres_hook.get_sqlalchemy_engine()
        
        # Load to staging table
        df.to_sql('itsm_raw_tickets', engine, if_exists='replace', index=False)
        
        logging.info(f"Loaded {len(df)} records to PostgreSQL staging table")
        
    except Exception as e:
        logging.error(f"Error in data loading: {str(e)}")
        raise

def run_data_quality_checks(**context):
    """
    Run comprehensive data quality checks
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Check for duplicate incident numbers
        duplicate_check = postgres_hook.get_first("""
            SELECT COUNT(*) as duplicates 
            FROM (
                SELECT inc_number, COUNT(*) 
                FROM itsm_raw_tickets 
                GROUP BY inc_number 
                HAVING COUNT(*) > 1
            ) t
        """)[0]
        
        if duplicate_check > 0:
            logging.warning(f"Found {duplicate_check} incidents with duplicate numbers")
        
        # Check resolution time outliers
        outlier_check = postgres_hook.get_first("""
            SELECT COUNT(*) as outliers
            FROM itsm_raw_tickets 
            WHERE resolution_time_hours > 720 OR resolution_time_hours < 0
        """)[0]
        
        if outlier_check > 0:
            logging.warning(f"Found {outlier_check} tickets with unusual resolution times")
        
        logging.info("Data quality checks completed")
        
    except Exception as e:
        logging.error(f"Error in data quality checks: {str(e)}")
        raise

# Task definitions
extract_data_task = PythonOperator(
    task_id='extract_itsm_data',
    python_callable=extract_itsm_data,
    dag=dag,
)

create_staging_table_task = PostgresOperator(
    task_id='create_staging_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS itsm_raw_tickets (
        inc_business_service TEXT,
        inc_category TEXT,
        inc_number TEXT,
        inc_priority TEXT,
        inc_sla_due TEXT,
        inc_sys_created_on TIMESTAMP,
        inc_resolved_at TIMESTAMP,
        inc_assigned_to TEXT,
        inc_state TEXT,
        inc_cmdb_ci TEXT,
        inc_caller_id TEXT,
        inc_short_description TEXT,
        inc_assignment_group TEXT,
        inc_close_code TEXT,
        inc_close_notes TEXT,
        resolution_time_hours NUMERIC
    );
    """,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

data_quality_task = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

run_dbt_task = BashOperator(
    task_id='run_dbt_transformations',
    bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .',
    dag=dag,
)

run_dbt_tests_task = BashOperator(
    task_id='run_dbt_tests',
    bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
    dag=dag,
)

# Task dependencies
extract_data_task >> create_staging_table_task >> load_data_task >> data_quality_task
data_quality_task >> run_dbt_task >> run_dbt_tests_task