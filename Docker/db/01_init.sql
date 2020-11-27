---------------------------------------------------
-- OVERALL DATABASE & SCHEMA
---------------------------------------------------

CREATE DATABASE interlocutor;
GRANT ALL PRIVILEGES ON DATABASE interlocutor TO docker_user;
\connect interlocutor;


---------------------------------------------------
-- THE GUARDIAN ARTICLES
---------------------------------------------------

CREATE SCHEMA the_guardian;
GRANT ALL PRIVILEGES ON SCHEMA the_guardian TO docker_user;

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


-- Content
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
