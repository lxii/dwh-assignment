CREATE SCHEMA IF NOT EXISTS PROD; 
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

CREATE TABLE IF NOT EXISTS PROD.TEAMS (
    team_id UUID PRIMARY KEY,
    group_activity TEXT NOT NULL,
    country_code CHAR(3) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS PROD.MEMBERSHIPS (
    membership_id UUID PRIMARY KEY,
    group_id UUID NOT NULL,
    role_title TEXT CHECK (role_title IN ('member', 'admin')),
    joined_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (group_id) REFERENCES PROD.TEAMS(team_id)
);

CREATE TABLE IF NOT EXISTS PROD.EVENTS (
    event_id UUID PRIMARY KEY,
    team_id UUID NOT NULL,
    event_start TIMESTAMP WITH TIME ZONE NOT NULL,
    event_end TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (team_id) REFERENCES PROD.TEAMS(team_id)
);

CREATE TABLE IF NOT EXISTS PROD.EVENT_RSVPS (
    event_rsvp_id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    member_id UUID NOT NULL,
    rsvp_status INTEGER CHECK (rsvp_status IN (0, 1, 2)),
    responded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (event_id) REFERENCES PROD.EVENTS(event_id),
    FOREIGN KEY (member_id) REFERENCES PROD.MEMBERSHIPS(membership_id)
);

-- helper table for analytics
CREATE TABLE IF NOT EXISTS SILVER.EVENT_DATES AS 
    WITH dates AS (
        SELECT
            dd::date as event_date
            , extract('isoyear' from current_date)::int as event_year
            , extract('month' from current_date)::int as event_month
            , extract('week' from current_date)::int as event_week
        FROM generate_series
            ( '2020-01-01'::date, '2030-01-01'::date, '1 day'::interval) dd
) SELECT 
    event_date
    , event_year
    , event_month
    , event_week
FROM dates;

\copy PROD.TEAMS(team_id, group_activity, country_code, created_at) FROM '/tmp/db-setup/teams_data.csv' WITH CSV HEADER;
\copy PROD.MEMBERSHIPS(membership_id, group_id, role_title, joined_at) FROM '/tmp/db-setup/memberships_data.csv' WITH CSV HEADER;
\copy PROD.EVENTS(event_id, team_id, event_start, event_end, created_at) FROM '/tmp/db-setup/events_data.csv' WITH CSV HEADER;
\copy PROD.EVENT_RSVPS(event_rsvp_id, event_id, member_id, rsvp_status, responded_at) FROM '/tmp/db-setup/event_rsvps_data.csv' WITH CSV HEADER;
