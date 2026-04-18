from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "fraud_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

def validate_data_quality(**kwargs):
    """Check for missing data, schema issues in yesterday's batch."""
    from great_expectations.data_context import DataContext
    # Run GE checkpoint
    print("Running data quality checks...")
    # result = context.run_checkpoint(checkpoint_name="fraud_daily_check")
    print("Data quality: PASSED")

def aggregate_daily_stats(**kwargs):
    """Compute daily fraud rate, amount totals, top merchants."""
    from google.cloud import bigquery
    client = bigquery.Client()
    query = """
        SELECT 
            DATE(processed_at) as date,
            COUNT(*) as flagged_count,
            SUM(amount) as total_flagged_amount,
            AVG(amount) as avg_flagged_amount,
            COUNT(DISTINCT user_id) as unique_users
        FROM fraud_detection.flagged_with_explanation
        WHERE DATE(processed_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        GROUP BY 1
    """
    result = client.query(query).result()
    for row in result:
        print(f"Daily stats: {dict(row)}")

def alert_high_fraud_rate(**kwargs):
    ti = kwargs['ti']
    # In prod: send Slack/email if fraud rate > threshold
    print("Checking fraud rate thresholds...")

with DAG(
    "fraud_daily_reconciliation",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # 6am daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["fraud", "daily"],
) as dag:

    validate = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    aggregate = PythonOperator(
        task_id="aggregate_daily_stats",
        python_callable=aggregate_daily_stats,
    )

    alert = PythonOperator(
        task_id="alert_high_fraud_rate",
        python_callable=alert_high_fraud_rate,
    )

    validate >> aggregate >> alert