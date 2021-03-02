from snowflake_connector import SnowflakeConnector
import os

snowflake_connection_details = {
    "user": os.environ.get("SNOWFLAKE_USER"),
    "role": os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    "account": os.environ.get("SNOWFLAKE_ACCOUNT") + "." + os.environ.get("SNOWFLAKE_REGION", "eu-west-1"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    }

snowflake_instance = SnowflakeConnector(snowflake_connection_details)
cursor = snowflake_instance.set_session_parameters(
    role="SYSADMIN", warehouse="COMPUTE_WH"
    )
create_db_films = snowflake_instance.run_sql(cursor, f"CREATE DATABASE IF NOT EXISTS FILMS_{os.environ.get('ENV')};")
create_schema_genre = snowflake_instance.run_sql(cursor, f"CREATE SCHEMA IF NOT EXISTS FILMS_{os.environ.get('ENV')}.GENRE;")
# create_table_romcom = snowflake_instance.run_sql(cursor, f"CREATE TABLE IF NOT EXISTS FILMS_{os.environ.get('ENV')}.GENRE.ROMCOM COL1 VARCHAR;")
data_putter = snowflake_instance.run_sql(cursor, f"PUT file://Users/paweljagoda/Documents/snowflake_project/netflix_titles.csv @%test_table")
data_copier = snowflake_instance.run_sql(cursor, f"COPY INTO test_table")
# result = sunowflake_instance.run_sql(cursor, "SHOW DATABASES;")
# df = snowflake_instance.fetch_dataframe_from_sql(cursor, "SHOW DATABASES;")

print(df)
