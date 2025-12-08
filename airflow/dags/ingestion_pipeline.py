from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.generate_data import DataGenerator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

SPARK_JARS_PACKAGES = "io.delta:delta-spark_2.12:3.0.0"

DERBY_JARS = "/opt/jars/derbyclient-10.14.2.0.jar"

SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.warehouse.dir": "/opt/datasets/warehouse",
    "spark.hadoop.fs.defaultFS": "file:///",
    "spark.executor.extraPythonPath": "/opt/spark_code",
    "spark.driver.extraPythonPath": "/opt/spark_code",
    "spark.executor.extraClassPath": "/opt/spark/jars/derbyclient-10.16.1.1.jar:/opt/spark/jars/derbyshared-10.16.1.1.jar:/opt/spark_code/config",
    "spark.sql.catalogImplementation": "hive",
    "spark.sql.hive.metastore.jars": "builtin",
    "spark.sql.hive.metastore.version": "2.3.9",
}
def generate_customer_data():
    print("Generating data")
    generator = DataGenerator(
        num_customers=1000,
        num_products=100,
        num_orders=2000,
        duplicate_percent=0.2,
        invalid_percent=0.05,
    )
    generator.generate_all()
    print("data generated")

with DAG(
    dag_id="etl_pipeline_medallion",
    default_args=default_args,
    description="ETL pipeline: Generate data -> Setup tables -> Raw -> Structured",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "delta", "etl", "medallion"],
) as dag:

    generate_data_task = PythonOperator(
        task_id="generate_customer_data",
        python_callable=generate_customer_data,
    )

    setup_tables_task = SparkSubmitOperator(
        task_id="setup_delta_tables",
        application="/opt/spark_code/config/setup_tables.py",
        conn_id="spark_default",
        packages=SPARK_JARS_PACKAGES,
        conf=SPARK_CONF,
        driver_class_path=DERBY_JARS + ":/opt/spark_code/config",
        verbose=True,
    )

    customers_source_to_raw = SparkSubmitOperator(
        task_id="customers_source_to_raw",
        application="/opt/spark_code/execute/source_to_raw/customers_source_to_raw.py",
        conn_id="spark_default",
        packages=SPARK_JARS_PACKAGES,
        conf=SPARK_CONF,
        verbose=True,
        driver_class_path=DERBY_JARS + ":/opt/spark_code/config",
        py_files="/opt/spark_code/spark_code.zip"
    )

    orders_source_to_raw = SparkSubmitOperator(
        task_id="orders_source_to_raw",
        application="/opt/spark_code/execute/source_to_raw/orders_source_to_raw.py",
        conn_id="spark_default",
        packages=SPARK_JARS_PACKAGES,
        conf=SPARK_CONF,
        verbose=True,
        driver_class_path=DERBY_JARS + ":/opt/spark_code/config",
        py_files="/opt/spark_code/spark_code.zip"
    )

    products_source_to_raw = SparkSubmitOperator(
        task_id="products_source_to_raw",
        application="/opt/spark_code/execute/source_to_raw/products_source_to_raw.py",
        conn_id="spark_default",
        packages=SPARK_JARS_PACKAGES,
        conf=SPARK_CONF,
        verbose=True,
        driver_class_path=DERBY_JARS + ":/opt/spark_code/config",
        py_files="/opt/spark_code/spark_code.zip"
    )

    generate_data_task >> setup_tables_task
    setup_tables_task >> customers_source_to_raw
    setup_tables_task >> [customers_source_to_raw, orders_source_to_raw, products_source_to_raw]

    # ==========================================================================
    # RAW TO STRUCTURED (Silver Layer)
    # ==========================================================================

    customers_raw_to_structured = SparkSubmitOperator(
        task_id="customers_raw_to_structured",
        application="/opt/spark_code/execute/raw_to_structured/customers_raw_to_structured.py",
        conn_id="spark_default",
        packages=SPARK_JARS_PACKAGES,
        conf=SPARK_CONF,
        verbose=True,
        driver_class_path=DERBY_JARS + ":/opt/spark_code/config",
        py_files="/opt/spark_code/spark_code.zip"
    )

    orders_raw_to_structured = SparkSubmitOperator(
        task_id="orders_raw_to_structured",
        application="/opt/spark_code/execute/raw_to_structured/orders_raw_to_structured.py",
        conn_id="spark_default",
        packages=SPARK_JARS_PACKAGES,
        conf=SPARK_CONF,
        verbose=True,
        driver_class_path=DERBY_JARS + ":/opt/spark_code/config",
        py_files="/opt/spark_code/spark_code.zip"
    )

    products_raw_to_structured = SparkSubmitOperator(
        task_id="products_raw_to_structured",
        application="/opt/spark_code/execute/raw_to_structured/products_raw_to_structured.py",
        conn_id="spark_default",
        packages=SPARK_JARS_PACKAGES,
        conf=SPARK_CONF,
        verbose=True,
        driver_class_path=DERBY_JARS + ":/opt/spark_code/config",
        py_files="/opt/spark_code/spark_code.zip"
    )

    customers_source_to_raw >> customers_raw_to_structured
    orders_source_to_raw >> orders_raw_to_structured
    products_source_to_raw >> products_raw_to_structured