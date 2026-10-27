import sys
PROJECT_ROOT = "/Users/duytrung/Documents/project"
if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime,timedelta
from airflow.sdk import task, DAG,task_group
from src.discover import get_pending_months
import subprocess
from airflow.sdk.exceptions import AirflowFailException,AirflowException


with DAG(
        dag_id="nyc_taxi_pipeline",
        start_date=datetime(2025, 1, 1),
        schedule="@monthly",
        max_active_tasks=1,
        max_active_runs=1,
        catchup=False
) as dag:

        @task(retries=2, retry_delay=timedelta(minutes=1))
        def run_clean(month):
                result = subprocess.run([
                        sys.executable,
                        'src/clean.py',
                        '--month',
                        month
                ],
                check = False,
                cwd=PROJECT_ROOT
        )
                returncode = result.returncode

                if returncode == 0:
                        print("SUCCESS")
                        return month
                elif returncode == 1:
                        raise AirflowException("Retryable pipeline failure")

                elif returncode == 2:
                        raise AirflowFailException("Invalid pipeline data")

                elif returncode == 3:
                        raise AirflowFailException("Raw file not found")

                else:
                        raise AirflowException(f"Unknown exit code: {returncode}")
        @task
        def run_report(month):
                result = subprocess.run([
                        sys.executable,
                        'src/explore.py',
                        '--month',
                        month
                ],
                check = False,
                cwd=PROJECT_ROOT
        )
                returncode = result.returncode

                if returncode == 0:
                        print('SUCCESS')
                        return month
                else:
                        raise AirflowException(
                                f"Report failed with exit code: {returncode}"
                                )

        @task_group(group_id='process_month')
        def process_month(month):
                clean_result = run_clean(month)
                run_report(clean_result)
        @task
        def discover_pending_months():
                return get_pending_months()
        pending_months = discover_pending_months()

        monthly_pipeline = process_month.expand(month = pending_months)
