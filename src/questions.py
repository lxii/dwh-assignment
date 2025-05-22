from sqlalchemy import create_engine
import pandas as pd

# in production case the credentials will be taken from secret manager
pg_engine = create_engine("postgresql://admin:g638ab94C3WysbXBG@localhost:5432/spond_prod")
pg_connection = pg_engine.connect()

print("Daily active teams:")
print(pd.read_sql("""
        SELECT event_date, count(team_id) as teams
        FROM gold.event_stats
        GROUP BY 1
        ORDER BY 1
        LIMIT 100
    """, pg_connection))

print("RSVP Summary:")
print(pd.read_sql("""
        SELECT 
            event_date
            , event_id
            , n_unanswered
            , n_accepted
            , n_declined
        FROM gold.daily_rsvp_stats
        ORDER BY 2, 1
        LIMIT 100
    """, pg_connection))

print("Attendance Rate in the last 30 days:")
print(pd.read_sql("""
        SELECT 
            AVG(n_accepted/n_sent)
        FROM gold.daily_rsvp_stats
        WHERE event_date >= current_date - interval '30' day
    """, pg_connection))

print("New vs returning members:")
print(pd.read_sql("""
        WITH weekly_comparisons AS (                  
            SELECT
                member_id
                , event_year
                , event_week
                , n_accepted
                , COALESCE(
                    LAG(n_accepted, 1) OVER (
                        PARTITION BY member_id ORDER BY event_year, event_week
                        ),
                        0
                    ) AS n_accepted_last_week
            FROM gold.weekly_activity_stats
        )
        SELECT 
            event_year
            , event_week
            , CASE 
                WHEN n_accepted_last_week = 0 THEN 'new'
                WHEN n_accepted_last_week = 1 THEN 'returning'
              END AS user_type
            , COUNT(DISTINCT(member_id)) as n_users
        FROM weekly_comparisons
        WHERE n_accepted = 1
        GROUP by 1, 2, 3
        ORDER BY 1, 2, 3
    """, pg_connection))

pg_connection.close()
