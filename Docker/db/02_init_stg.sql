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
COPY the_guardian.article_content FROM '/staging_data/the_guardian.article_content.csv' WITH CSV HEADER;

-- This csv is empty to test scenario where an article has not been preprocessed yet
COPY the_guardian.article_content_bow_preprocessed
    FROM '/staging_data/the_guardian.article_content_bow_preprocessed.csv' WITH CSV HEADER;


---------------------------------------------------
-- DAILY MAIL ARTICLES
---------------------------------------------------

COPY daily_mail.columnists FROM '/staging_data/daily_mail.columnists.csv' WITH CSV HEADER;
COPY daily_mail.columnist_article_links
    FROM '/staging_data/daily_mail.columnist_article_links.csv' WITH CSV HEADER;
COPY daily_mail.article_content
    FROM '/staging_data/daily_mail.article_content.csv' WITH CSV HEADER;
COPY daily_mail.article_content_bow_preprocessed
    FROM '/staging_data/daily_mail.article_content_bow_preprocessed.csv' WITH CSV HEADER;


---------------------------------------------------
-- ENCODED REPRESENTATIONS OF ARTICLE CONTENT
---------------------------------------------------

-- Pre-existing vocabulary
COPY encoded_articles.tfidf_vocabulary FROM '/staging_data/encoded_articles.tfidf_vocabulary.csv' WITH CSV HEADER;

-- For staging only, replace this table with a version which uses the vocabulary from
-- encoded_articles.tfidf_vocabulary.csv
DROP TABLE encoded_articles.tfidf_representation;

CREATE TABLE encoded_articles.tfidf_representation
(
    id           VARCHAR PRIMARY KEY,
    "and"          FLOAT,
    content      FLOAT,
    other        FLOAT,
    preprocessed FLOAT,
    "some"         FLOAT,
    words        FLOAT
);

COMMENT ON TABLE encoded_articles.tfidf_representation IS 'tf-idf encoded representation of articles.';
COMMENT ON COLUMN encoded_articles.tfidf_representation.id IS 'Unique identifier (hash of article URL)';
COMMENT ON COLUMN encoded_articles.tfidf_representation.and IS 'td-idf for this word in document';
COMMENT ON COLUMN encoded_articles.tfidf_representation.content IS 'td-idf for this word in document';
COMMENT ON COLUMN encoded_articles.tfidf_representation.other IS 'td-idf for this word in document';
COMMENT ON COLUMN encoded_articles.tfidf_representation.preprocessed IS 'td-idf for this word in document';
COMMENT ON COLUMN encoded_articles.tfidf_representation.some IS 'td-idf for this word in document';
COMMENT ON COLUMN encoded_articles.tfidf_representation.words IS 'td-idf for this word in document';

