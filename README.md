# Data Engineering Project using Snowflake

In this project I'm going to attempt connecting the code on my machine with my snowflake database using Python. This will involve writing a program that will act as a mediator between my computer and snowflake.  

## How to run the code

This project relies on environment variables for user authentication. You will need to export the following variables to your shell:

    SNOWFLAKE_USER
    SNOWFLAKE_ROLE
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_REGION
    SNOWFLAKE_WAREHOUSE

To run the code use:

    python3 query_snowflake.py
