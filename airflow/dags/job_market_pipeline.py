from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "Sujeet",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 27),
    "retries": 1,
}

with DAG(
    dag_id="Job_Market_Pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["Job_Market"],
) as dag:

    producer = BashOperator(
    task_id="Producer",
    bash_command="python3 /home/talentum/test-jupyter/Job_Market_Analytics/producer/producer.py"
)

    bronze = BashOperator(
        task_id="Bronze_Layer",
        bash_command="python3 /home/talentum/test-jupyter/Job_Market_Analytics/bronze.py"
    )

    silver = BashOperator(
        task_id="Silver_Layer",
        bash_command="python3 /home/talentum/test-jupyter/Job_Market_Analytics/silver.py"
    )

    gold = BashOperator(
        task_id="Gold_Layer",
        bash_command="python3 /home/talentum/test-jupyter/Job_Market_Analytics/gold.py"
    )

    producer >> bronze >> silver >> gold
