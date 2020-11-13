-- Copy staging data so tests can be run against the same typ of tables and data that exists in production
\connect interlocutor;

---------------------------------------------------
-- MOCK DATA FOR SIMPLE TESTING
---------------------------------------------------

CREATE SCHEMA testing_schema;
GRANT ALL PRIVILEGES ON SCHEMA testing_schema TO $POSTGRES_USER;

-- Metadata
CREATE TABLE testing_schema.testing_table
(
    example_integer   INT,
    example_string    VARCHAR,
    example_timestamp TIMESTAMP
);

COMMENT ON TABLE testing_schema.testing_table IS 'Mock table for testing database access.';
COMMENT ON COLUMN testing_schema.testing_table.example_integer IS 'For testing handling of integers';
COMMENT ON COLUMN testing_schema.testing_table.example_string IS 'For testing handling of strings';
COMMENT ON COLUMN testing_schema.testing_table.example_timestamp IS 'For testing handling of timestamps';

COPY testing_schema.testing_table FROM '/staging_data/testing_schema.testing_table.csv' WITH CSV HEADER;


---------------------------------------------------
-- THE GUARDIAN ARTICLES
---------------------------------------------------

COPY the_guardian.article_metadata FROM '/staging_data/the_guardian.article_metadata.csv' WITH CSV HEADER;