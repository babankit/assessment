import snowflake.snowpark as snowpark
from snowflake.snowpark import functions as f

import logging

logger = logging.getLogger("student_transformer")


def get_student_df(session: snowpark.Session):
    """
    Read data from snowflake stage.
    Reading Student Data from snowflake stage.

    Args:
        session: snowpark.Session Snowflake Connection

    Note:
        1. This method is uploaded in form of stored procedure and run via airflow
    """
    students_df = session.read.json("@stage_student_data/students.txt").select(
        f.sql_expr("$1:students").alias("students")
    )
    student_df = students_df.flatten("students").select_expr(
        """
            VALUE:student_id::string as student_id, 
            VALUE:name::string as name,
            VALUE:grades.english::integer as english_grade,
            VALUE:grades.history::integer as history_grade,
            VALUE:grades.math::integer as math_grade,
            VALUE:grades.science::integer as science_grade"""
    )
    return student_df


def get_missed_class_df(session: snowpark.Session):
    """
    Read data from snowflake stage.
    Reading Missed Class Data from snowflake stage

    Args:
        session: snowpark.Session Snowflake Connection

    Note:
        1. This method is uploaded in form of stored procedure and run via airflow
    """
    missed_classes_df = session.read.json(
        "@stage_student_data/missed_classes.txt"
    ).select(f.sql_expr("$1:missed_classes").alias("classes"))
    missed_class_df = missed_classes_df.flatten("classes").select_expr(
        """VALUE:student_id::string as student_id, 
           VALUE:missed_days::integer as missed_days"""
    )
    return missed_class_df


def main(session: snowpark.Session):
    """
    Read both datasets students and missed_classes and join them on student_id
    Write data in form of table STUDENTS_STAGING.STUDENTS_MERGED_DB.FINAL_MERGED

    Args:
        session: snowpark.Session Snowflake Connection

    Note:
        1. This method is invoked in form of stored procedure and run via airflow
    """
    student_df = get_student_df(session=session)
    missed_class_df = get_missed_class_df(session=session)
    joined_df = student_df.join(missed_class_df, on="student_id", how="left")

    joined_df.write.mode("append").save_as_table(
        "STUDENTS_STAGING.STUDENTS_MERGED_DB.FINAL_MERGED"
    )
    return "COMPLETED"
