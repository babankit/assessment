USE ROLE USERADMIN;

DELETE ROLE IF EXISTS STUDENT_ANALYSER;
DELETE USER TESTUSER;
    


-- SYSADMIN RESPONSIBILITIES
USE ROLE SYSADMIN;
DROP DATABASE IF EXISTS students_semantic;
DROP SCHEMA IF EXISTS students_semantic.views_students;

DROP DATABASE IF EXISTS students_staging;
DROP SCHEMA IF EXISTS students_staging.students_merged_db;
