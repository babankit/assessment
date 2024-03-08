export WORKING_DIR=$(pwd)


# Inject environment variables
export ACCOUNT_NAME=
export DB_NAME=
export SCHEMA_NAME=
export ROLE_NAME=
export WAREHOUSE=
export USERNAME=


# Uploads scripts to snowflake (can be viewed via snowsight)
snowsql \
  --accountname $ACCOUNT_NAME \
  --dbname $DB_NAME \
  --schemaname $SCHEMA_NAME \
  --rolename $ROLE_NAME \
  --warehouse $WAREHOUSE \
  --username $USERNAME \
  --query "PUT file:///${WORKING_DIR}/student_transformer.py @STUDENTS_STAGING.STUDENTS_MERGED_DB.STAGE_SCRIPTS/student_transformer AUTO_COMPRESS=FALSE OVERWRITE=TRUE

snowsql \
  --accountname $ACCOUNT_NAME \
  --dbname $DB_NAME \
  --schemaname $SCHEMA_NAME \
  --rolename $ROLE_NAME \
  --warehouse $WAREHOUSE \
  --username $USERNAME \
  --query "PUT file:///${WORKING_DIR}/semantic_math_view.py @STUDENTS_SEMANTIC.VIEWS_STUDENTS.VIEW_SCRIPTS/semantic_math_view AUTO_COMPRESS=FALSE OVERWRITE=TRUE
