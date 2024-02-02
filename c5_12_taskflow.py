import uuid

import airflow
import pendulum

from airflow import DAG
from airflow.decorators import task


with DAG(
    dag_id="c5_12_taskflow",
    start_date=pendulum.today('UTC').add(days=-3),
    schedule="@daily",
) as dag:

    @task
    def train_model():
        model_id = str(uuid.uuid4())
        return model_id

    @task
    def deploy_model(model_id: str):
        print(f"Deploying model {model_id}")

    model_id = train_model()
    deploy_model(model_id)
