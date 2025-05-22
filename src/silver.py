from sqlalchemy import create_engine, text

# in production case the credentials will be taken from secret manager
DB_URL = "postgresql://admin:g638ab94C3WysbXBG@localhost:5432/spond_prod"
SOURCE_SCHEMA = "BRONZE"
TARGET_SCHEMA = "SILVER"

PG_ENGINE = create_engine(DB_URL)


PROCESSING_CONFIG = [
    (
        "TEAMS",
        f"""
            CREATE TABLE {TARGET_SCHEMA}.TEAMS
            AS
            SELECT
                team_id
                , group_activity
                , country_code
                , DATE_TRUNC('minute', created_at) as created_at
            FROM {SOURCE_SCHEMA}.TEAMS
        """
    ),
    (
        "MEMBERSHIPS",
        f"""
            CREATE TABLE {TARGET_SCHEMA}.MEMBERSHIPS
            AS
            SELECT
                membership_id AS member_id
                , group_id AS team_id
                , role_title
                , DATE_TRUNC('minute', joined_at at time zone 'UTC') as joined_at 
            FROM {SOURCE_SCHEMA}.MEMBERSHIPS
        """
    ),
    (
        "EVENTS",
        f"""
            CREATE TABLE {TARGET_SCHEMA}.EVENTS
            AS
            SELECT
                event_id
                , team_id
                , DATE_TRUNC('minute', event_start at time zone 'UTC') as event_start
                , DATE_TRUNC('minute', event_end at time zone 'UTC') as event_end
                , DATE_TRUNC('minute', created_at at time zone 'UTC') as created_at
            FROM {SOURCE_SCHEMA}.EVENTS
        """
    ),
    (
        "EVENT_RSVPS",
        f"""
            CREATE TABLE {TARGET_SCHEMA}.EVENT_RSVPS
            AS
            SELECT
                event_rsvp_id
                , event_id
                , member_id
                , rsvp_status
                , DATE_TRUNC('minute', responded_at at time zone 'UTC') as responded_at 
            FROM {SOURCE_SCHEMA}.EVENT_RSVPS
        """
    )
]


def copy_to_silver(engine, table, query):
    with engine.begin() as connection:
        print(f"PROCESSING {SOURCE_SCHEMA}.{table} to {TARGET_SCHEMA}.{table}")
        connection.execute(text(query))
        print("Done!")


def main():
    for table_config in PROCESSING_CONFIG:

        table, create_statement = table_config

        try:
            copy_to_silver(PG_ENGINE, table, create_statement)

            # validate that the bronze tables have data
        except Exception as e:
            print(f"Error copying {table}: {e}")


if __name__ == "__main__":
    main()
