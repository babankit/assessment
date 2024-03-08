from snowflake.snowpark import Session
from snowflake.snowpark import functions as f

connection_parameters = {
    "account": "ye58953.ap-southeast-1",
    "user": "TESTUSER",
    "password": "password",
    "role": "STUDENT_ANALYSER",  # optional
    "warehouse": "COMPUTE_WH",  # optional
    "database": "STUDENTS_SEMANTIC",  # optional
    "schema": "VIEWS_STUDENTS",  # optional
}  

session = Session.builder.configs(connection_parameters).create()

student_merged_df = session.table("STUDENTS_STAGING.STUDENTS_MERGED_DB.FINAL_MERGED")
filtered_df = student_merged_df.filter(f.col("MATH_GRADE")>90)
w_students_view = filtered_df.create_or_replace_view("STUDENTS_SEMANTIC.VIEWS_STUDENTS.W_STUDENTS")

session.close()