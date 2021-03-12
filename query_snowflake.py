import os
import pandas as pd
from snowflake_connector import SnowflakeConnector

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
create_db_films = snowflake_instance.run_sql(cursor, f"CREATE DATABASE IF NOT EXISTS FILMS_{os.environ.get('ENV', 'DEV')};")
result = snowflake_instance.run_sql(cursor, "SHOW DATABASES;")
df = snowflake_instance.fetch_dataframe_from_sql(cursor, "SHOW DATABASES;")
print(df)

create_schema_genre = snowflake_instance.run_sql(cursor, f"CREATE SCHEMA IF NOT EXISTS FILMS_{os.environ.get('ENV', 'DEV')}.GENRE;")

table_ddl_statement = '''"show_id" VARCHAR, "type" VARCHAR, "title" VARCHAR, "director" VARCHAR, "cast" VARCHAR, "country" VARCHAR, "date_added" VARCHAR, "release_year" VARCHAR, "rating" VARCHAR, "duration" VARCHAR, "listed_in" VARCHAR, "description" VARCHAR'''

create_table_netflix = snowflake_instance.run_sql(cursor, f"CREATE TABLE IF NOT EXISTS FILMS_{os.environ.get('ENV', 'DEV')}.GENRE.NETFLIX_CONTENT ({table_ddl_statement});")
data_putter = snowflake_instance.run_sql(cursor, f"PUT file://netflix_titles.csv @~ auto_compress=false")
read = snowflake_instance.run_sql(cursor, f"LIST @~")
data_copier = snowflake_instance.run_sql(cursor, f"""COPY INTO FILMS_{os.environ.get('ENV', 'DEV')}.GENRE.NETFLIX_CONTENT from @~/netflix_titles.csv FILE_FORMAT = ( TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY ='"' SKIP_HEADER = 1)""")

select_all_from_netflix = snowflake_instance.run_sql(cursor, f"SELECT * FROM FILMS_{os.environ.get('ENV', 'DEV')}.GENRE.NETFLIX_CONTENT;")

df = pd.DataFrame.from_records(select_all_from_netflix)

movies = df[df.type == "Movie"]
shows = df[df.type == "TV Show"]

max_length = movies.duration.apply(lambda value : int(value.split(" ")[0])).max()

movies['duration_cleaned'] = movies.duration.apply(lambda value : int(value.split(" ")[0]))

print(movies[movies.duration_cleaned == max_length])
