from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def run_spark_job():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("AirflowSparkSubmitTest")
        .master("spark://spark-master:7077")
        .config("spark.driver.host", "airflow-worker")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .getOrCreate()
    )

    print("==================================================")
    print("  SPARK JOB STARTED VIA AIRFLOW PYTHON OPERATOR   ")
    print("==================================================")

    data = [
        ("Airflow 3", "Orchestration", 100),
        ("Apache Spark 3.5.0", "Distributed Computing", 200),
        ("Debian 13", "Infrastructure", 300),
    ]

    df = spark.createDataFrame(data, ["Component", "Role", "Score"])
    df.show()

    print("==================================================")
    print("  JOB COMPLETED SUCCESSFULLY!                     ")
    print("==================================================")

    spark.stop()


with DAG(
    dag_id="02_spark_submit_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "test"],
) as dag:

    submit_spark_job = PythonOperator(
        task_id="submit_job_to_spark_cluster",
        python_callable=run_spark_job,
    )