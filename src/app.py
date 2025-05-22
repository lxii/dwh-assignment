from sqlalchemy import create_engine
import pandas as pd

# in production case the credentials will be taken from secret manager
pg_engine = create_engine("postgresql://admin:g638ab94C3WysbXBG@localhost:5432/spond_prod")
pg_connection = pg_engine.connect()

print('python is ready')

#    CREATE TABLE IF NOT EXISTS data_mart AS 
#    WITH dates AS (


membership_stats = """
    SELECT
        d.event_date
        , m.group_id
        , COUNT(DISTINCT(membership_id)) as n_total_members
        , COUNT(DISTINCT(membership_id)) FILTER (WHERE role_title = 'admin') AS n_admins
        , COUNT(DISTINCT(membership_id)) FILTER (WHERE role_title = 'member') AS n_members
    FROM prod.memberships m
    CROSS JOIN ANALYTICS.event_dates d
    WHERE 1=1
    AND d.event_date >= m.joined_at::date
    AND group_id = 'c98e5090-0633-4fdd-9484-4c5fb38dfcb7'
    GROUP BY 1, 2
    ORDER BY 1
    LIMIT 100;
"""


data_mart = """
    WITH event_dates AS (
        -- create a series of event_dates for further analytical use
        SELECT
            dd::date as event_date
            , extract('isoyear' from current_date)::int as event_year
            , extract('month' from current_date)::int as event_month
            , extract('week' from current_date)::int as event_week
        FROM generate_series
            ( '2023-01-01'::date, '2025-06-01'::date, '1 day'::interval) dd
    ), spond_event_stats AS (
        -- create daily series for spond events with the grain of event_id + event_date
        SELECT
            d.event_date
            , d.event_week
            , d.event_month
            , d.event_year
            , e.team_id
            , e.event_id
        FROM prod.events e
        CROSS JOIN event_dates d
        WHERE 1=1
        AND d.event_date >= e.event_start::date
        AND d.event_date <= e.event_end::date
    ), membership_stats AS (
        -- get the team compositions (# members and admins) by date
        SELECT
            d.event_date
            , m.group_id
            , COUNT(DISTINCT(membership_id)) as n_total_members
            , COUNT(DISTINCT(membership_id)) FILTER (WHERE role_title = 'admin') AS n_admins
            , COUNT(DISTINCT(membership_id)) FILTER (WHERE role_title = 'member') AS n_members
        FROM prod.memberships m
        CROSS JOIN event_dates d
        WHERE 1=1
        AND d.event_date >= m.joined_at::date
    )
    SELECT *
    FROM spond_event_stats;
"""

print(pd.read_sql(membership_stats, pg_connection))
print(pd.read_sql("select min(joined_at::date), max(joined_at::date) from prod.memberships", pg_connection))
# print(pd.read_sql("select count(distinct(event_id)) from prod.events", pg_connection))

pg_connection.close()
