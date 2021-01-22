---------------------------------------------------
-- OVERALL DATABASE & SCHEMA
---------------------------------------------------

CREATE DATABASE interlocutor;
GRANT ALL PRIVILEGES ON DATABASE interlocutor TO $POSTGRES_USER;
\connect interlocutor;


---------------------------------------------------
-- THE GUARDIAN ARTICLES
---------------------------------------------------

CREATE SCHEMA the_guardian;
GRANT ALL PRIVILEGES ON SCHEMA the_guardian TO $POSTGRES_USER;

-- Metadata
CREATE TABLE the_guardian.article_metadata
(
    id                        CHAR(32) PRIMARY KEY,
    guardian_id               VARCHAR,
    content_type              VARCHAR,
    section_id                VARCHAR,
    section_name              VARCHAR,
    web_publication_timestamp TIMESTAMP,
    web_title                 VARCHAR,
    web_url                   VARCHAR,
    api_url                   VARCHAR,
    pillar_id                 VARCHAR,
    pillar_name               VARCHAR
);

COMMENT ON TABLE the_guardian.article_metadata IS 'Metadata, including how to access content.';
COMMENT ON COLUMN the_guardian.article_metadata.id IS 'Hash of guardian_id to create unique identifier of fixed length';
COMMENT ON COLUMN the_guardian.article_metadata.guardian_id IS 'Path to the article content';
COMMENT ON COLUMN the_guardian.article_metadata.content_type IS 'Type of article e.g. interactive, video, or article';
COMMENT ON COLUMN the_guardian.article_metadata.section_id IS 'ID of the section in The Guardian';
COMMENT ON COLUMN the_guardian.article_metadata.section_name IS 'Name of the section in The Guardian';
COMMENT ON COLUMN the_guardian.article_metadata.web_publication_timestamp IS 'Combined date and time of publication';
COMMENT ON COLUMN the_guardian.article_metadata.web_title IS 'Article title/headline';
COMMENT ON COLUMN the_guardian.article_metadata.web_url IS 'URL of the html content';
COMMENT ON COLUMN the_guardian.article_metadata.api_url IS 'URL of the raw content';
COMMENT ON COLUMN the_guardian.article_metadata.pillar_id IS 'High-level section ID';
COMMENT ON COLUMN the_guardian.article_metadata.pillar_name IS 'High-level section name';


-- Raw article content
CREATE TABLE the_guardian.article_content
(
    id                        CHAR(32) PRIMARY KEY,
    guardian_id               VARCHAR,
    web_publication_timestamp TIMESTAMP,
    api_url                   VARCHAR,
    content                   VARCHAR,
    CONSTRAINT fk_id
      FOREIGN KEY(id)
	  REFERENCES the_guardian.article_metadata(id)
);

COMMENT ON TABLE the_guardian.article_content IS 'Text content of articles.';
COMMENT ON COLUMN the_guardian.article_content.id IS 'Hash of guardian_id to create unique identifier of fixed length';
COMMENT ON COLUMN the_guardian.article_content.guardian_id IS 'Path to the article content';
COMMENT ON COLUMN the_guardian.article_content.web_publication_timestamp IS 'Combined date and time of publication';
COMMENT ON COLUMN the_guardian.article_content.api_url IS 'URL of the raw content';
COMMENT ON COLUMN the_guardian.article_content.content IS 'Text content of the article';


-- Preprocessed content for recent articles by each columnist for use in bag of words models
CREATE TABLE the_guardian.article_content_bow_preprocessed
(
    id      CHAR(32) PRIMARY KEY,
    processed_content VARCHAR,
    CONSTRAINT fk_id
      FOREIGN KEY(id)
	  REFERENCES the_guardian.article_content(id)
);

COMMENT ON TABLE the_guardian.article_content_bow_preprocessed IS 'Text content of articles.';
COMMENT ON COLUMN the_guardian.article_content_bow_preprocessed.id IS 'Unique identifier of article (hash of guardian_id)';
COMMENT ON COLUMN the_guardian.article_content_bow_preprocessed.processed_content IS 'Processed text content of article';


---------------------------------------------------
-- THE DAILY MAIL ARTICLES
---------------------------------------------------

CREATE SCHEMA daily_mail;
GRANT ALL PRIVILEGES ON SCHEMA daily_mail TO $POSTGRES_USER;

-- Columnists
CREATE TABLE daily_mail.columnists
(
    columnist VARCHAR PRIMARY KEY,
    homepage  VARCHAR
);

COMMENT ON TABLE daily_mail.columnists IS 'Columnist names and their homepage on the website.';
COMMENT ON COLUMN daily_mail.columnists.columnist IS 'Name of the writer';
COMMENT ON COLUMN daily_mail.columnists.homepage IS 'URL of the columnist homepage';


-- Links to recent articles by each columnist
CREATE TABLE daily_mail.columnist_article_links
(
    columnist  VARCHAR,
    article_id CHAR(32) PRIMARY KEY,
    url        VARCHAR
);

COMMENT ON TABLE daily_mail.columnist_article_links IS 'Links to recent articles by columnist.';
COMMENT ON COLUMN daily_mail.columnist_article_links.columnist IS 'Name of the writer';
COMMENT ON COLUMN daily_mail.columnist_article_links.article_id IS 'Hash of url to create unique identifier of fixed length';
COMMENT ON COLUMN daily_mail.columnist_article_links.url IS 'Link to the article';


-- Content for recent articles by each columnist
CREATE TABLE daily_mail.article_content
(
    id      CHAR(32) PRIMARY KEY,
    url     VARCHAR,
    title   VARCHAR,
    content VARCHAR,
    CONSTRAINT fk_id
      FOREIGN KEY(id)
	  REFERENCES daily_mail.columnist_article_links(article_id)
);

COMMENT ON TABLE daily_mail.article_content IS 'Text content of recent articles by columnists.';
COMMENT ON COLUMN daily_mail.article_content.id IS 'Hash of url to create unique identifier of fixed length';
COMMENT ON COLUMN daily_mail.article_content.url IS 'Link to article';
COMMENT ON COLUMN daily_mail.article_content.title IS 'Title of article';
COMMENT ON COLUMN daily_mail.article_content.content IS 'Text content of article';


-- Preprocessed content for recent articles by each columnist for use in bag of words models
CREATE TABLE daily_mail.article_content_bow_preprocessed
(
    id      CHAR(32) PRIMARY KEY,
    processed_content VARCHAR,
    CONSTRAINT fk_id
      FOREIGN KEY(id)
	  REFERENCES daily_mail.article_content(id)
);

COMMENT ON TABLE daily_mail.article_content_bow_preprocessed IS 'Text content of recent articles by columnists.';
COMMENT ON COLUMN daily_mail.article_content_bow_preprocessed.id IS 'Unique identifier (hash of article URL)';
COMMENT ON COLUMN daily_mail.article_content_bow_preprocessed.processed_content IS 'Processed text content of article';


---------------------------------------------------
-- ENCODED REPRESENTATIONS OF ARTICLE CONTENT
---------------------------------------------------

CREATE SCHEMA encoded_articles;
GRANT ALL PRIVILEGES ON SCHEMA encoded_articles TO $POSTGRES_USER;

-- Vocabulary across all articles
CREATE TABLE encoded_articles.tfidf_vocabulary
(
    word                  VARCHAR PRIMARY KEY,
    feature_matrix_index  INTEGER
);

COMMENT ON TABLE encoded_articles.tfidf_vocabulary IS 'Mapping showing every distinct word in the preprocessed version of articles and their index in the tf-idf feature matrix.';
COMMENT ON COLUMN encoded_articles.tfidf_vocabulary.word IS 'Individual word from corpus across all articles';
COMMENT ON COLUMN encoded_articles.tfidf_vocabulary.feature_matrix_index IS 'Index within the feature matrix';


-- tf-idf encoded version of each article
CREATE TABLE encoded_articles.tfidf_representation
(
    id      CHAR(32) PRIMARY KEY,
    encoded FLOAT ARRAY
);

COMMENT ON TABLE encoded_articles.tfidf_representation IS 'tf-idf encoded representation of articles.';
COMMENT ON COLUMN encoded_articles.tfidf_representation.id IS 'Unique identifier (hash of article URL)';
COMMENT ON COLUMN encoded_articles.tfidf_representation.encoded IS 'tf-idf vector representation of article content. The word that each index applies to can be found in encoded_articles.tfidf_vocabulary';


-- Similar article pairs
CREATE TABLE encoded_articles.tfidf_similar_articles
(
    id                 CHAR(32),
    similar_article_id CHAR(32),
    similarity_score   FLOAT,
    PRIMARY KEY (id, similar_article_id)
);

COMMENT ON TABLE encoded_articles.tfidf_similar_articles IS 'Articles which are similar to one another.';
COMMENT ON COLUMN encoded_articles.tfidf_similar_articles.id IS 'Unique identifier (hash of article URL)';
COMMENT ON COLUMN encoded_articles.tfidf_similar_articles.similar_article_id IS 'Unique identifier of similar article';
COMMENT ON COLUMN encoded_articles.tfidf_similar_articles.similarity_score IS 'Cosine similarity between two articles';
