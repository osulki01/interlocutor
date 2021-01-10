"""
Testing the encoding/embedding of text so it is represented in a form which can be used by machine learning algorithms.
"""

# Third party imports
import pandas as pd
import pytest

# Internal imports
from interlocutor.nlp import encoding
from interlocutor.database import postgresql


class TestTfidfEncoder:
    """Testing for TfidfEncoder class."""

    @pytest.mark.integration
    def test_analyse_and_overwrite_existing_vocabulary(self, monkeypatch):
        """Vocabulary is extracted from preprocessed version of articles and stored to database."""

        # Establish encoder which loads a mock version of article content
        def mock_preprocessed_content():
            """Mock representation of preprocessed articles."""

            return pd.DataFrame(data={
                'id': ['article_1', 'article_2'],
                'processed_content': ['some words', 'some more words']
            })

        tfidf_encoder = encoding.TfidfEncoder()
        monkeypatch.setattr(tfidf_encoder, '_load_all_articles_bow_preprocessed_content', mock_preprocessed_content)

        tfidf_encoder._analyse_and_overwrite_existing_vocabulary()

        # # Query the table to see if the new rows were inserted correctly
        expected_vocabulary = pd.DataFrame(data={'word': ['more', 'some', 'words'], 'feature_matrix_index': [0, 1, 2]})

        db_connection = postgresql.DatabaseConnection()
        db_connection._create_connection()

        with db_connection._conn.cursor() as cursor:
            cursor.execute("SELECT * FROM encoded_articles.tfidf_vocabulary;")

            table_tuples = cursor.fetchall()
            actual_vocabulary = pd.DataFrame(data=table_tuples, columns=['word', 'feature_matrix_index'])

            # Tidy up and revert to original version of table
            cursor.execute("TRUNCATE TABLE encoded_articles.tfidf_vocabulary;")

            cursor.execute(
                """
                COPY encoded_articles.tfidf_vocabulary
                FROM '/staging_data/encoded_articles.tfidf_vocabulary.csv'
                WITH CSV HEADER;
                """
            )

            db_connection._conn.commit()
        db_connection._close_connection()

        pd.testing.assert_frame_equal(actual_vocabulary, expected_vocabulary)

    @pytest.mark.integration
    def test_load_all_articles_bow_preprocessed_content(self):
        """The bag of words preprocessed version of all articles from every publication is loaded correctly."""

        # Staging data only exists for the daily mail, so only one article should be retrieved
        expected_content = pd.DataFrame(
            data={'id': ['3587c1cb3b85d116d9573897437fc4db'], 'processed_content': ['some preprocessed content']}
        )

        tfidf_encoder = encoding.TfidfEncoder()
        actual_content = tfidf_encoder._load_all_articles_bow_preprocessed_content()

        pd.testing.assert_frame_equal(expected_content, actual_content)

    @pytest.mark.integration
    def test_load_existing_vocabulary(self):
        """Existing vocabulary is loaded in as a mapping between word and feature matrix index."""

        expected_vocabulary = {
            'and': 0,
            'content': 1,
            'other': 2,
            'preprocessed': 3,
            'some': 4,
            'words': 5,
        }

        tfidf_encoder = encoding.TfidfEncoder()
        actual_vocabulary = tfidf_encoder._load_existing_vocabulary()

        assert actual_vocabulary == expected_vocabulary
