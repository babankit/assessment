CREATE OR REPLACE PROCEDURE STUDENTS_STAGING.STUDENTS_MERGED_DB.STAGE_SCRIPTS
RETURNS string
LANGUAGE python
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = ('@STUDENTS_STAGING.STUDENTS_MERGED_DB.STAGE_SCRIPTS/student_transformer/student_transformer.py')
HANDLER = 'student_transformer.main'
EXECUTE AS STUDENT_ANALYSER;


CREATE OR REPLACE PROCEDURE STUDENTS_SEMANTIC.VIEWS_STUDENTS.VIEW_SCRIPTS
RETURNS string
LANGUAGE python
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = ('@STUDENTS_SEMANTIC.VIEWS_STUDENTS.VIEW_SCRIPTS/semantic_math_view/semantic_math_view.py')
HANDLER = 'semantic_math_view.main'
EXECUTE AS STUDENT_ANALYSER;