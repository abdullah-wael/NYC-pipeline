from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import datetime
import os
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
@dag(
    schedule="@daily",
    start_date=datetime.datetime(2026, 9, 16),
    catchup=False,
    tags=["NYC_taxi"],
)
def NYC_taxi():
    extract_and_store = SparkSubmitOperator(
        task_id="extract_and_store",
        application="/opt/airflow/spark_apps/to_minio.py",
        conn_id="spark_default",
        jars="/opt/airflow/hadoop-aws-3.3.4.jar,/opt/airflow/aws-java-sdk-bundle-1.12.262.jar",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": os.environ["AWS_ACCESS_KEY_ID"],
            "spark.hadoop.fs.s3a.secret.key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
    )

    transform = SparkSubmitOperator(
        task_id="transform",
        application="/opt/airflow/spark_apps/silver.py",
        conn_id="spark_default",
        jars="/opt/airflow/hadoop-aws-3.3.4.jar,/opt/airflow/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/spark-snowflake_2.12-3.2.2-spark_3.5.jar,/opt/airflow/snowflake-jdbc-4.0.2.jar",
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": os.environ["AWS_ACCESS_KEY_ID"],
            "spark.hadoop.fs.s3a.secret.key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
    )
    DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
    DBT_DIR = "/opt/airflow/dbt_project/nyc_taxi"
    DBT_ENV = (
    "DBT_LOG_PATH=/tmp/dbt_logs "
    "DBT_TARGET_PATH=/tmp/dbt_target "
    "DBT_PACKAGES_INSTALL_PATH=/tmp/dbt_packages "
)

    gold = BashOperator(
    task_id="dbt_run",
    bash_command=f"cd {DBT_DIR} && {DBT_ENV} {DBT_BIN} run --profiles-dir .",
    )
    test=BashOperator(
        task_id="test",
        bash_command=f"cd {DBT_DIR} && {DBT_ENV} {DBT_BIN} test --profiles-dir ."
    )
    email_success = EmailOperator(
        task_id="email_success",
        conn_id="smtp",
        from_email="abdowael2392005@gmail.com",
        to="abdowael2392005@gmail.com",
        subject="NYC taxi pipeline finished",
        html_content="<p>your NYC taxi pipeline has completed successfully.</p>",
    )

   
    
    extract_and_store >> transform >> gold >> test>> email_success


NYC_taxi()  