"""
Testing the encoding/embedding of text so it is represented in a form which can be used by machine learning algorithms.
"""

# Standard libraries
import os

# Third party libraries
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

        tfidf_encoder._analyse_and_overwrite_existing_vocabulary(
            mock_preprocessed_content()['processed_content'].values
        )

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

    @pytest.mark.parametrize("use_existing_vocab", [True, False])
    @pytest.mark.integration
    def test_encode_articles(self, use_existing_vocab):
        """
        Articles are represented with a tf-idf matrix either using a pre-existing dictionary or one which has been
        rebuilt.
        """

        # Load what we are expecting to see, which is the tf-idf representation of the only article which has already
        # been bag-of-words preprocessed in the staging data: daily_mail.article_content_bow_preprocessed
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Load expected tf-idf representation of the mock article. The csv files have been calculated following the
        # the sklearn documentation here:
        # https://scikit-learn.org/stable/modules/feature_extraction.html#tfidf-term-weighting

        # Scenario 1: Using an existing vocabulary (pulled from existing table)
        if use_existing_vocab:
            expected_tfidf = pd.read_csv(
                f'{script_directory}/test_encode_articles__expected_using_existing_vocab.csv'
            )

        # Scenario 2: Creating a new vocabulary from scratch
        else:
            expected_tfidf = pd.read_csv(
                f'{script_directory}/test_encode_articles__expected_not_using_existing_vocab.csv'
            )

        # Encode articles and check the output
        tfidf_encoder = encoding.TfidfEncoder(use_existing_vocab=use_existing_vocab)
        tfidf_encoder.encode_articles()

        db_connection = postgresql.DatabaseConnection()
        db_connection._create_connection()

        with db_connection._conn.cursor() as cursor:
            cursor.execute("SELECT * FROM encoded_articles.tfidf_representation;")

            table_tuples = cursor.fetchall()
            actual_tfidf = pd.DataFrame(data=table_tuples, columns=list(expected_tfidf.columns))

            db_connection._conn.commit()

        # Tidy up and revert to original version of tables
        with db_connection._conn.cursor() as cursor:

            # tf-idf matrix representation of each article (which was empty to begin with)
            cursor.execute("DROP TABLE encoded_articles.tfidf_representation;")
            cursor.execute(
                """
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
                """
            )

            # Vocabulary table
            cursor.execute('TRUNCATE TABLE encoded_articles.tfidf_vocabulary;')
            cursor.execute(
                """
                COPY encoded_articles.tfidf_vocabulary
                FROM '/staging_data/encoded_articles.tfidf_vocabulary.csv'
                WITH CSV HEADER;
                """
            )

            db_connection._conn.commit()
        db_connection._close_connection()

        # Check output
        pd.testing.assert_frame_equal(actual_tfidf, expected_tfidf)

    @pytest.mark.parametrize("use_existing_vocab", [False, True])
    @pytest.mark.integration
    def test_load_all_articles_bow_preprocessed_content(self, use_existing_vocab):
        """The bag of words preprocessed version of all articles from every publication is loaded correctly."""

        # Staging data only exists for the daily mail, so only one article should be retrieved
        expected_content = pd.DataFrame(
            data={'id': ['3587c1cb3b85d116d9573897437fc4db'], 'processed_content': ['some preprocessed content']}
        )

        tfidf_encoder = encoding.TfidfEncoder(use_existing_vocab=use_existing_vocab)
        actual_content = tfidf_encoder._load_all_articles_bow_preprocessed_content()

        pd.testing.assert_frame_equal(expected_content, actual_content)

    @pytest.mark.integration
    def test_load_vocabulary(self):
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
        actual_vocabulary = tfidf_encoder._load_vocabulary()

        assert actual_vocabulary == expected_vocabulary
