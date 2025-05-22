from sqlalchemy import create_engine, text

# in production case the credentials will be taken from secret manager
DB_URL = "postgresql://user:password@localhost:5432/appdb"
TARGET_SCHEMA = "GOLD"

PG_ENGINE = create_engine("postgresql://admin:g638ab94C3WysbXBG@localhost:5432/spond_prod")

ANALYTICS_CONFIG = [
    (
        "membership_stats",
        """
            SELECT
                d.event_date
                , d.event_week
                , d.event_month
                , d.event_year
                , m.group_id
                , COUNT(DISTINCT(membership_id)) as n_total_members
                , COUNT(DISTINCT(membership_id)) FILTER (WHERE role_title = 'admin') AS n_admins
                , COUNT(DISTINCT(membership_id)) FILTER (WHERE role_title = 'member') AS n_members
            FROM SILVER.MEMBERSHIPS m
            CROSS JOIN SILVER.EVENT_DATES d
            WHERE 1=1
            AND d.event_date >= m.joined_at::date
            AND group_id = 'c98e5090-0633-4fdd-9484-4c5fb38dfcb7'
            GROUP BY 1, 2
        """
    ),
    (
        "event_stats",
        """
            SELECT
                d.event_date
                , d.event_week
                , d.event_month
                , d.event_year
                , e.team_id
                , e.event_id
            FROM SILVER.EVENTS e
            CROSS JOIN SILVER.EVENT_DATES d d
            WHERE 1=1
            AND d.event_date >= e.event_start::date
            AND d.event_date <= e.event_end::date
        """
    ),
    (
        "team_composition_stats",
        """
            SELECT
                d.event_date
                , d.event_week
                , d.event_month
                , d.event_year
                , m.group_id
                , COUNT(membership_id) as n_total_members
                , COUNT(membership_id) FILTER (WHERE role_title = 'admin') AS n_admins
                , COUNT(membership_id) FILTER (WHERE role_title = 'member') AS n_members
            FROM SILVER.MEMBERSHIPS m
            CROSS JOIN SILVER.EVENT_DATES d
            WHERE 1=1
            AND d.event_date >= m.joined_at::date"
        """
    ),
    (
        "daily_rsvp_stats",
        """
            SELECT
                event_date
                , d.event_week
                , d.event_month
                , d.event_year
                , event_id
                , COUNT(membership_id) FILTER (WHERE rsvp_status = 0) AS n_unanswered
                , COUNT(membership_id) FILTER (WHERE rsvp_status = 1) AS n_accepted
                , COUNT(membership_id) FILTER (WHERE rsvp_status = 2) AS n_declined
            FROM SILVER.EVENT_RSVPS r
            CROSS JOIN SILVER.EVENT_DATES d
            WHERE responded_at::date = d.event_date
            GROUP BY 1, 2, 3, 4, 5
        """
    ),
    (
        "weekly_retention_stats",
        """
            SELECT
                    r.member_id
                    , t.team_id
                    , d.event_week
                    , count(1) AS n_accepted_events
                FROM SILVER.EVENT_RSVPS r
                JOIN SILVER.MEMBERSHIPS m ON r.member_id = t.team_id
                CROSS JOIN SILVER.EVENT_DATES d
            WHERE responded_at::date = d.event_date
            AND rsvp_status = 1
        """
    )
]


def create_view(engine, view, query):
    with engine.begin() as connection:

        create_statement = f"""
            CREATE OR REPLACE VIEW {view}
            AS {query}
        """

        print(f"Creating view {TARGET_SCHEMA}.{view}.")
        connection.execute(text(create_statement))
        print("Done!")


def main():
    for view_config in ANALYTICS_CONFIG:

        view, query = view_config

        try:
            create_view(PG_ENGINE, view, query)

            # validate that the bronze tables have data
        except Exception as e:
            print(f"Error creating view {view}: {e}")


if __name__ == "__main__":
    main()
