import snowflake.snowpark as snowpark
from snowflake.snowpark import functions as f

import logging

logger = logging.getLogger("semantic_math_view")


def main(session: snowpark.Session):
    """
    Creates view on top of STUDENTS_STAGING.STUDENTS_MERGED_DB.FINAL_MERGED
    for students having math grade > 90

    Args:
        session: snowpark.Session Snowflake Connection

    Note:
        1. This method is invoked in form of stored procedure and run via airflow
    """
    try:
        student_merged_df = session.table(
            "STUDENTS_STAGING.STUDENTS_MERGED_DB.FINAL_MERGED"
        )
        filtered_df = student_merged_df.filter(f.col("MATH_GRADE") > 90)
        w_students_view = filtered_df.create_or_replace_view(
            "STUDENTS_SEMANTIC.VIEWS_STUDENTS.W_STUDENTS"
        )

        logger.info(f"View Created Successfully. {str(w_students_view)}")
        return str(w_students_view)

    except Exception as ex:
        logger.error(f"View Creation failed. {str(ex)}")
