import pandas as pd
from databricks import sql  # ✅ Ensure this import is present
import snowflake.connector
def connect_to_snowflake(sf_config):
    return snowflake.connector.connect(
        user=sf_config["user"],
        account=sf_config["account"],
        authenticator=sf_config["authenticator"],
        database=sf_config["database"],
        schema=sf_config["schema"]
    )

def connect_to_databricks(db_config):
    return sql.connect(
        server_hostname=db_config["server_hostname"],
        http_path=db_config["http_path"],
        access_token=db_config["access_token"]
    )
