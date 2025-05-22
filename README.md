# Warehousing case

I chose to use PostgreSQL as a data source and a data warehouse. The data ingestion and procedures are written in SQL (mostly) and Python.

## 1. Setting up the project

In order to run the project clone the repo, `cd` into the folder and run the following commands (requires podman and python):

`bash podman-setup.sh`
`bash pg-init.sh`

Once you see the CREATE TABLE and COPY statements in the terminal, you can run the ingestion using the following command:

`bash etl.sh`

This command will run the ETL (not templated by dates, just a wholesale processing of tables and creation of views).

It will output the samples of data answering analytical questions.

After it is done running, please run the cleanup script:

`bash cleanup.sh`

## 2. Data model

The data model follows a [medallion architecture](https://www.databricks.com/glossary/medallion-architecture).

First, the data is copied from production to bronze database.

Then in silver columns are renamed for consistency, timestamps are truncated to minutes and converted to UTC, assuming that is a system timezone.

In gold layer the analytical views are created.

To-do: the following data quality checks can be enabled that can lead to insights about production issues:

1. general constraint violations: checking how many constraints (null or out-of-check values).
2. groups without admins.
3. outdated event rsvps after the event dates.

### Handling deletions

Depending on the production architecture we can do the following to optimize the deletions from the data warehouse:

1. Index (z-order) on the keys subject to deletion.
2. Hashing (using surrogate ids), anonymizing and obfuscating the dataset along with extracting identifiable information into a smaller dimension. This will enable to run deletions on smaller dimension, removing the reidentification risk from remaining datasets.

### Handling changes

In order to capture the cases when users accept or reject the event multiple times, it is better to use slowly changing dimension type 2. In this case, in ETL we don't update the records based on `event_id` + `member_id`, instead we add a new row for each change.

That way we can track historical signup rate for events.

## 3. Production considerations

1. Scalability: the increase in production traffic will affect the implementation in two ways:

    * instead of creating analytical views, the data in data marts can be persisted and inserted as daily batches.

    * data ingestion: instead of querying the massive amounts of data from the production database, data pipeline can consume write-ahead logs or other exports. In addition, the cadence of ingestion can be switched to more frequent updates if daily batches are expensive for the production database.

    * data processing: the tables will need to be properly partitioned, so that unnecessary data is skipped on querying. The prime candidates for different tables can be `country_code` and `*_date` or `*_month` fields derived from various timestamps fields. In addition, `members`, `events` and `event_rsvps` can be denormalized to include the `country_code` and then be partitioned with it. Also, if the data is in a several terrabyte range, then a switch to big-data platform might be required. That is using cloud storage and distributed compute framework, i.e. pyspark for processing.

    * adding smaller integer keys (or surrogate ids) and providing indexing (or z-order for delta or iceberg tables) on them.

2. Schema evolution and versioning:
    * first of all, it will require a setting of ground rules with developers, data engineer(s), and analysts, e.g. not using `SELECT *` from databases, not renaming existing fields if it can be avoided, and not changing the granularity of the tables. This way even with the addition of new columns the existing reports will not crash.
    * a process can be set up monitoring production schema changes and validating them against the analytical databases.
    * in general, maintaining a direct communication line with the product/tech and analysts will ensure that the team is proactive, and not reactive.

3. Testing: assuming CI/CD should be a part of the 

4. Monitoring: enabling the production logs for the compute and orchestration environments, recording the numbers of inserted/updated/deleted rows for each pipeline run, having basic slack/teams/email integrations notifying about pipeline failures.

5. GDPR

In line with privacy by design, the following measures will need to be implemented:

* a data retention routine: a daily script that will delete data beyond the retention period.
* precision reduction: timestamps should be reduced to minutes, date of births reduced to years.
* if the business needs allow it, replace natural id's (teams, members, events, rsvp's) with surrogate ids, while maintaining a separate mapping with original for operational needs. Surrogate id's provide the same level of obfuscation as cryptographic hashing, but are more difficult to re-identify because they are not directly related to the original business key.
* moreover, the separation of different access levels for data ingestion/data processing levels.
