from sqlalchemy import create_engine, text

# define global variables
DB_URL = "postgresql://admin:g638ab94C3WysbXBG@localhost:5432/spond_prod"
SOURCE_SCHEMA = "PROD"
TARGET_SCHEMA = "BRONZE"

TABLES = ["TEAMS", "MEMBERSHIPS", "EVENTS", "EVENT_RSVPS"]

PG_ENGINE = create_engine(DB_URL)


def copy_table(engine, schema_from, schema_to, table_name):
    print(f"Copying data from {schema_from}.{table_name} to {schema_to}.{table_name}.")

    with engine.begin() as connection:
        # to be parametrized with event_date/fresh id's in production
        copy_sql = text(f"""
            CREATE TABLE {schema_to}.{table_name} AS
            SELECT *
            FROM {schema_from}.{table_name};
        """)

        connection.execute(copy_sql)
        print("Done!")


def main():
    for table in TABLES:
        try:
            copy_table(PG_ENGINE, SOURCE_SCHEMA, TARGET_SCHEMA, table)

            # validate that the bronze tables have data
        except Exception as e:
            print(f"Error copying {table}: {e}")


if __name__ == "__main__":
    main()
