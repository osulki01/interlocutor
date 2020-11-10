-- Copy staging data so tests can be run against the same typ of tables and data that exists in production
\connect interlocutor;

COPY the_guardian.article_metadata FROM '/staging_data/the_guardian.article_metadata.csv' WITH CSV HEADER;