"""
Testing the encoding/embedding of text so it is represented in a form which can be used by machine learning algorithms.
"""

# Standard libraries
import ast
import os

# Third party libraries
import numpy as np
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

    def test_calculate_similarities(self, monkeypatch):
        """Cosine similarity matrix is calculated correctly."""

        def mock_tfidf_encoding(**kwargs):
            """Mock an encoded version of different articles. Use 1's and 0's for simplicity."""

            # Dataframe with encoded articles based upon a vocabulary of 4 words
            return pd.DataFrame(
                columns=['id', 'encoded'],
                data=[
                    # Article 1 and 2 should be identical
                    ['article_1', [1, 0, 0, 0]],
                    ['article_2', [1, 0, 0, 0]],
                    # Article 3 and 4 are similar but not identical
                    ['article_3', [0, 0, 1, 0]],
                    ['article_4', [0, 1, 1, 0]],
                    # Article_5 is not similar to any
                    ['article_5', [0, 0, 0, 1]],
                ]
            )

        # Create encoder which will work with a mock representation of articles
        tfidf_encoder = encoding.TfidfEncoder()
        monkeypatch.setattr(tfidf_encoder._db_connection, 'get_dataframe', mock_tfidf_encoding)

        # Analyse and save the similarity between all of the articles against one another
        actual_similarity = tfidf_encoder._calculate_similarities()

        # Check output, cosine similarity calculated using
        # https://scikit-learn.org/stable/modules/generated/sklearn.metrics.pairwise.cosine_similarity.html
        expected_similarity = pd.DataFrame(
            columns=['id', 'article_1', 'article_2', 'article_3', 'article_4', 'article_5'],
            data=[
                # Article 1 and 2 should be identical
                ['article_1', 1.0, 1.0, 0.0, 0.0, 0.0],
                ['article_2', 1.0, 1.0, 0.0, 0.0, 0.0],
                # Article 3 and 4 are similar but not identical
                ['article_3', 0.0, 0.0, 1.0, 0.70710, 0.0],
                ['article_4', 0.0, 0.0, 0.70710, 1.0, 0.0],
                # Article_5 is not similar to any
                ['article_5', 0.0, 0.0, 0.0, 0.0, 1.0],
            ]
        ).set_index('id')

        pd.testing.assert_frame_equal(actual_similarity, expected_similarity, check_names=False)

    @pytest.mark.parametrize("use_existing_vocab", [True, False])
    @pytest.mark.integration
    def test_encode_articles(self, use_existing_vocab):
        """
        Articles are represented with a tf-idf matrix either using a pre-existing dictionary or one which has been
        rebuilt.
        """

        db_connection = postgresql.DatabaseConnection()
        db_connection._create_connection()

        # Clear existing staging data
        with db_connection._conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE encoded_articles.tfidf_representation;")
            db_connection._conn.commit()

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

        with db_connection._conn.cursor() as cursor:
            cursor.execute("SELECT * FROM encoded_articles.tfidf_representation;")

            table_tuples = cursor.fetchall()
            actual_tfidf = pd.DataFrame(data=table_tuples, columns=list(expected_tfidf.columns))

            db_connection._conn.commit()

        # Tidy up and revert to original version of tables
        with db_connection._conn.cursor() as cursor:

            # tf-idf matrix representation of each article
            cursor.execute("TRUNCATE TABLE encoded_articles.tfidf_representation;")
            cursor.execute(
                """
                COPY encoded_articles.tfidf_representation
                FROM '/staging_data/encoded_articles.tfidf_representation.csv'
                WITH CSV HEADER;
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

        # The encoded values is loaded from the csv as a string, so convert back to an array
        expected_tfidf_vector = np.array(ast.literal_eval(expected_tfidf['encoded'][0]))

        assert actual_tfidf.index == expected_tfidf.index

        np.testing.assert_almost_equal(actual=actual_tfidf['encoded'][0], desired=expected_tfidf_vector, decimal=6)

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

    @pytest.mark.parametrize('similarity_threshold', [0.5, 0.8])
    @pytest.mark.integration
    def test_store_most_similar_articles(self, similarity_threshold):
        """Appropriately similar articles are picked up and saved."""

        tfidf_encoder = encoding.TfidfEncoder()

        # Check how article pairs are found and saved to database
        tfidf_encoder.store_most_similar_articles(similarity_threshold=similarity_threshold)

        # The tf-idf representation of the data can be found in
        # Docker/db/staging_data/encoded_articles.tfidf_representation.csv and is stored in the table
        # encoded_articles.tfidf_representation

        # Using that mock data;
        # article 1 and 2 should be identical
        # article 3 and 4 are similar but not identical
        # article_5 is not similar to any

        db_connection = postgresql.DatabaseConnection()
        db_connection._create_connection()

        with db_connection._conn.cursor() as cursor:
            cursor.execute("SELECT * FROM encoded_articles.tfidf_similar_articles;")

            table_tuples = cursor.fetchall()
            actual_article_pairs = pd.DataFrame(
                data=table_tuples,
                columns=['id', 'similar_article_id', 'similarity_score']
            )

            # Tidy up and revert to original version of table
            cursor.execute("TRUNCATE TABLE encoded_articles.tfidf_similar_articles;")

            db_connection._conn.commit()

        # Check whether pairs found match what we expect
        if similarity_threshold == 0.5:

            expected_article_pairs = pd.DataFrame(
                columns=['id', 'similar_article_id', 'similarity_score'],
                data=[
                    ['article_1_d36c525d1679623119fd7a', 'article_2_4b2a76b9719d911017c592', 1.0],
                    ['article_2_4b2a76b9719d911017c592', 'article_1_d36c525d1679623119fd7a', 1.0],
                    ['article_3_8350295550de7d587bc323', 'article_4_1702e282b59c30e3789ad4', 0.70710],
                    ['article_4_1702e282b59c30e3789ad4', 'article_3_8350295550de7d587bc323', 0.70710]
                ]
            )

        # similarity_threshold == 0.8
        else:
            expected_article_pairs = pd.DataFrame(
                columns=['id', 'similar_article_id', 'similarity_score'],
                data=[
                    ['article_1_d36c525d1679623119fd7a', 'article_2_4b2a76b9719d911017c592', 1.0],
                    ['article_2_4b2a76b9719d911017c592', 'article_1_d36c525d1679623119fd7a', 1.0],
                ]
            )

        # Account for the id column being listed as a CHAR(32) data type in the database so remove padded characters to
        # make it 32 characters
        actual_article_pairs['id'] = actual_article_pairs['id'].str.strip()
        actual_article_pairs['similar_article_id'] = actual_article_pairs['similar_article_id'].str.strip()

        pd.testing.assert_frame_equal(actual_article_pairs, expected_article_pairs)

    @pytest.mark.parametrize('similarity_threshold', [-1, 1, 2])
    def test_store_most_similar_articles_expects_appropriate_threshold(self, similarity_threshold):
        """Exception is raised if similarity threshold is not between 0 and 1."""

        with pytest.raises(
                expected_exception=ValueError,
                match=r'similarity_threshold should be between 0 <= threshold < 1'
        ):
            tfidf_encoder = encoding.TfidfEncoder()
            tfidf_encoder.store_most_similar_articles(similarity_threshold=similarity_threshold)
