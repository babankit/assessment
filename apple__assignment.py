from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
import logging

from snowflake.snowpark import Session
from snowflake.snowpark import functions as f

logger = logging.getLogger("apple__assignment")

students_data_path = "res/students.txt"
missing_classes_path = "res/missed_classes.txt"

staging_database = "STUDENTS_STAGING"
staging_schema = "STUDENTS_MERGED_DB"
stage = f"{staging_database}.{staging_schema}"

view_database = "STUDENTS_SEMANTIC"
view_schema = "VIEWS_STUDENTS"
view = f"{view_database}.{view_schema}"


connection_parameters = {
    "account": "ye58953.ap-southeast-1",
    "user": Variable.get("snowflake_user"),
    "password": Variable.get("snowflake_password", deserialize_json=False),
    "role": "STUDENT_ANALYSER",  # optional
    "warehouse": "compute_wh",  # optional
    "database": "STUDENTS_STAGING",  # optional
    "schema": "STUDENTS_MERGED_DB",  # optional
}


# Loads Data in the specified snowflake stage
def load_data_in_stage(
    file_path,
):
    try:
        session = Session.builder.configs(connection_parameters).create()
        # Create stage if not exists.
        _ = session.sql(
            f"create stage if not exists {stage}.stage_student_data"
        ).collect()
        response = session.file.put(
            file_path, "@stage_student_data", auto_compress=False, overwrite=True
        )
        if response[0].status == "UPLOADED":
            return
        else:
            logger.error(f"Data Upload Failed, {response[0].message}")
    except Exception as ex:
        logger.warn("Caught exception while uploading data to stage : ")
        logger.error(str(ex))
    finally:
        session.close()


# Define your Python functions
def failure_step():
    # Notify via Email/Slack Integration
    pass


# Define the DAG
dag = DAG(
    "apple__assignment",
    description="Assignment DAG",
    schedule_interval="0 12 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["ingestion", "apple", "hcl"],
)


# Load Data
load_students_data = PythonOperator(
    task_id="load_students_data",
    python_callable=load_data_in_stage,
    op_kwargs={"file_path": students_data_path},
    dag=dag,
)

load_missed_classes_data = PythonOperator(
    task_id="load_missed_classes_data",
    python_callable=load_data_in_stage,
    op_kwargs={"file_path": missing_classes_path},
    dag=dag,
)

# Run Student Transformer Procedure
student_transformer = SnowflakeOperator(
    task_id="student_transformer",
    sql=f"CALL {stage}.stage_student_data;",
    session_parameters=None,
    snowflake_conn_id="snowflake_default",
    warehouse="compute_wh",
    database="STUDENTS_STAGING",
    role="STUDENT_ANALYSER",
    schema="STUDENTS_MERGED_DB",
    dag=dag,
)

# Run View Creator Script procedure
view_creator = SnowflakeOperator(
    task_id="view_creator",
    sql=f"CALL {view}.view_scripts;",
    session_parameters=None,
    snowflake_conn_id="snowflake_default",
    warehouse="compute_wh",
    database="STUDENTS_SEMANTIC",
    role="STUDENT_ANALYSER",
    schema="VIEWS_STUDENTS",
    dag=dag,
)

# Define the failure step task
failure_step_task = PythonOperator(
    task_id="failure_step",
    python_callable=failure_step,
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
)


load_students_data >> load_missed_classes_data >> student_transformer >> view_creator
load_students_data >> failure_step_task
load_missed_classes_data >> failure_step_task
student_transformer >> failure_step_task
view_creator >> failure_step_task
