from datetime import datetime
from docker.types import Mount
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="smart_meters",
    start_date=datetime(2024, 4, 24),
    schedule_interval=None,
    catchup=False,
    tags=["jakluz-de4d.2.5"],
) as dag:
    transformation = DockerOperator(
        task_id="spark_transformation",
        image="spark:python3",
        container_name="smart_meters_spark_transformation",
        auto_remove="force",
        network_mode="turing",
        environment={
            "PROCESSING_DATE": "2013-06-11",
        },
        mounts=[
            Mount(
                "/opt/spark/work-dir/src",
                "/home/ubuntu/jakluz-DE.4D.2.5/src",
                "bind",
            ),
            Mount(
                "/opt/spark/work-dir/data",
                "/home/ubuntu/jakluz-DE.4D.2.5/data",
                "bind",
            ),
        ],
        command="/opt/spark/bin/spark-submit /opt/spark/work-dir/src/data_transformation.py",
    )

    machine_learning = DockerOperator(
        task_id="spark_machine_learning_extraction",
        image="spark:python3",
        container_name="smart_meters_spark_machine_learning_extraction",
        auto_remove="force",
        network_mode="turing",
        environment={
            "PROCESSING_DATE": "2013-06-11",
        },
        mounts=[
            Mount(
                "/opt/spark/work-dir/src",
                "/home/ubuntu/jakluz-DE.4D.2.5/src",
                "bind",
            ),
            Mount(
                "/opt/spark/work-dir/data",
                "/home/ubuntu/jakluz-DE.4D.2.5/data",
                "bind",
            ),
        ],
        command="/opt/spark/bin/spark-submit /opt/spark/work-dir/src/machine-learning_data_extraction.py",
    )

    data_analytics_extraction = DockerOperator(
        task_id="spark_data_analytics_extraction",
        image="spark:python3",
        container_name="smart_meters_spark_data_analytics_extraction",
        auto_remove="force",
        network_mode="turing",
        environment={
            "PROCESSING_DATE": "2013-06-11",
        },
        mounts=[
            Mount(
                "/opt/spark/work-dir/src",
                "/home/ubuntu/jakluz-DE.4D.2.5/src",
                "bind",
            ),
            Mount(
                "/opt/spark/work-dir/data",
                "/home/ubuntu/jakluz-DE.4D.2.5/data",
                "bind",
            ),
        ],
        command="/opt/spark/bin/spark-submit /opt/spark/work-dir/src/data-analytics_data_extraction.py",
    )

    data_analytics_load_to_db = DockerOperator(
        task_id="spark_data_analytics_move_from_lake_to_db",
        image="spark:python3",
        container_name="smart_meters_spark_data_analytics_move_from_lake_to_db",
        auto_remove="force",
        network_mode="turing",
        environment={
            "PROCESSING_DATE": "2013-06-11",
            "DB_HOST": "{{ conn.get('postgres_turing_smart_meters').host }}",
            "DB_PORT": "{{ conn.get('postgres_turing_smart_meters').port }}",
            "DB_DATABASE": "{{ conn.get('postgres_turing_smart_meters').schema }}",
            "DB_USER": "{{ conn.get('postgres_turing_smart_meters').login }}",
            "DB_PASSWORD": "{{ conn.get('postgres_turing_smart_meters').password }}",
            "DB_DRIVER": "org.postgresql.Driver",
            "DB_TABLE": "smart_meters.analytics"
        },
        mounts=[
            Mount(
                "/opt/spark/work-dir/src",
                "/home/ubuntu/jakluz-DE.4D.2.5/src",
                "bind",
            ),
            Mount(
                "/opt/spark/work-dir/data",
                "/home/ubuntu/jakluz-DE.4D.2.5/data",
                "bind",
            ),
            Mount(
                "/opt/spark/jars/postgresql-42.7.3.jar",
                "/home/ubuntu/jakluz-DE.4D.2.5/postgresql-42.7.3.jar",
                "bind",
            ),
        ],
        command="/opt/spark/bin/spark-submit /opt/spark/work-dir/src/data-analytics_move_from_lake_to_db.py",
    )

transformation >> [machine_learning, data_analytics_extraction]
data_analytics_extraction >> data_analytics_load_to_db
