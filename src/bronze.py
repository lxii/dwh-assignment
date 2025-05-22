from sqlalchemy import create_engine, text
import pandas as pd

# define global variables
DB_URL = "postgresql://user:password@localhost:5432/appdb"
SOURCE_SCHEMA = "PROD"
TARGET_SCHEMA = "BRONZE"

TABLES = ["TEAMS", "MEMBERSHIPS", "EVENTS", "EVENT_RSVPS"]

PG_ENGINE = create_engine("postgresql://admin:g638ab94C3WysbXBG@localhost:5432/spond_prod")
PG_CONNECTION = PG_ENGINE.connect()


def copy_table(connection, schema_from, schema_to, table_name):
    print(f"Copying data from {schema_from}.{table_name} to {schema_to}.{table_name}...")

    # to be parametrized with event_date/fresh id's in production
    copy_sql = text(f"""
        CREATE TABLE {schema_to}.{table_name} AS
        SELECT *
        FROM {schema_from}.{table_name}
    """)

    connection.execute(copy_sql)


def main():
    for table in TABLES:
        try:
            copy_table(PG_CONNECTION, SOURCE_SCHEMA, TARGET_SCHEMA, table)
            print(pd.read_sql(f"SELECT * FROM BRONZE.{table} LIMIT 10", PG_CONNECTION))
        except Exception as e:
            print(f"Error copying {table}: {e}")

    PG_CONNECTION.close()


if __name__ == "__main__":
    main()
