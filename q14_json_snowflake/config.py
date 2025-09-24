

import snowflake.connector  # type: ignore

def connector():
    return snowflake.connector.connect(
        user="RITVISHAH",
        password="Ritviisthe098@",
        account="UVUMVOV-MXB37149",  # e.g., xy12345.us-east-1
        warehouse="COMPUTE_WH",
        database="SNOWFLAKE_DEMO_PRACTICE",
        schema="DEMO_SCHEMA"
    )
