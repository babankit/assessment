from snowflake.snowpark import Session
from snowflake.snowpark import functions as f

connection_parameters = {
    "account": "ye58953.ap-southeast-1",
    "user": "TESTUSER",
    "password": "password",
    "role": "STUDENT_ANALYSER",  # optional
    "warehouse": "COMPUTE_WH",  # optional
    "database": "STUDENTS_STAGING",  # optional
    "schema": "STUDENTS_MERGED_DB",  # optional
}  

session = Session.builder.configs(connection_parameters).create()

# Create stage if not exists.
_ = session.sql("create stage if not exists STUDENTS_STAGING.STUDENTS_MERGED_DB.stage_student_data").collect()

# _ = session.sql("create or replace temp stage stage_student_data").collect()
_ = session.file.put("res/students.txt", "@stage_student_data", auto_compress=False, overwrite=True)
# Create a DataFrame that uses a DataFrameReader to load data from a file in a snowflake stage.
students_df = session.read.json("@stage_student_data/students.txt").select(f.sql_expr("$1:students").alias("students"))
student_df = (students_df
       .flatten("students")
       .select_expr("""
                    VALUE:student_id::string as student_id, 
                    VALUE:name::string as name,
                    VALUE:grades.english::integer as english_grade,
                    VALUE:grades.history::integer as history_grade,
                    VALUE:grades.math::integer as math_grade,
                    VALUE:grades.science::integer as science_grade"""
       )
)


_ = session.file.put("res/missed_classes.txt", "@stage_student_data", auto_compress=False, overwrite=True)
# Create a DataFrame that uses a DataFrameReader to load data from a file in a stage.
missed_classes_df = session.read.json("@stage_student_data/missed_classes.txt").select(f.sql_expr("$1:missed_classes").alias("classes"))
missed_class_df = (missed_classes_df
       .flatten("classes")
       .select_expr("""
                    VALUE:student_id::string as student_id, 
                    VALUE:missed_days::integer as missed_days"""
       )
)

joined_df = student_df.join(missed_class_df, on="student_id", how="left")
joined_df.write.mode("append").save_as_table("STUDENTS_STAGING.STUDENTS_MERGED_DB.FINAL_MERGED")

session.close()