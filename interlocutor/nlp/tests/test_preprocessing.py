"""Testing for tidying and preprocessing text data."""

# Third party imports
import pandas as pd
import pytest

# Internal imports
from interlocutor.database import postgresql
from interlocutor.nlp import preprocessing


class TestBagOfWordsPreprocessor:
    """Test methods within the BagOfWordsPreprocessor class."""

    @pytest.mark.integration
    def test_preprocess_all_article_content(self):
        """Article content is preprocessed and uploaded to database."""

        preprocessor = preprocessing.BagOfWordsPreprocessor()
        preprocessor.preprocess_all_article_content()

        # Scenario 1
        # The daily mail article already has preprocessed content so no new rows will be added
        expected_daily_mail = pd.DataFrame(data={
            'id': ['3587c1cb3b85d116d9573897437fc4db'],
            'processed_content': 'some preprocessed content'
        })

        # Retrieve the table to see if it was populated correctly
        db_connection = postgresql.DatabaseConnection()
        db_connection._create_connection()

        with db_connection._conn.cursor() as curs:
            curs.execute('SELECT * FROM daily_mail.article_content_bow_preprocessed;')

            table_tuples = curs.fetchall()
            actual_daily_mail = pd.DataFrame(table_tuples, columns=['id', 'processed_content'])

            db_connection._conn.commit()

        pd.testing.assert_frame_equal(actual_daily_mail, expected_daily_mail)

        # Scenario 2
        # The guardian article has not been preprocessed yet and will be added
        expected_guardian = pd.DataFrame(data={
            'id': ['e8c5e312fae36c43d965a0e3da84e68d'],
            'processed_content': 'margaret thatcher britain female prime minister resign 22 november 1990'
        })

        # Retrieve the table to see if it was populated correctly
        db_connection = postgresql.DatabaseConnection()
        db_connection._create_connection()

        with db_connection._conn.cursor() as curs:
            curs.execute('SELECT * FROM the_guardian.article_content_bow_preprocessed;')

            table_tuples = curs.fetchall()
            actual_guardian = pd.DataFrame(table_tuples, columns=['id', 'processed_content'])

            # Only retain the first 10 words to compare against
            actual_guardian['processed_content'] = actual_guardian['processed_content'].apply(
                lambda x: ' '.join(x.split()[:10])
            )

            # Tidy up and delete newly inserted rows
            # (those that don't exist in the staging data Docker/db/staging_data/daily_mail.columnists.csv)
            curs.execute("TRUNCATE TABLE the_guardian.article_content_bow_preprocessed;")

            db_connection._conn.commit()

        pd.testing.assert_frame_equal(actual_guardian, expected_guardian)

    def test_preprocess_texts(self):
        """
        Texts are transformed appropriately i.e. stop words and punctuation are removed, then words are lemmatised and
        lowercased.
        """

        input_texts = [
            'This is a text which contains some stop words',
            'This text Contains some UPPERCASING ',
            'This text contains some high-profile punctuation! ...',
            'This text contains multiple spaces    in      it which need removing',
            'This text contains some words that require lemmatisation as we are playing with the processor class',
            'This text contains some contractions shan\'t it. Alicia\'s thinking so too',
        ]

        expected_output = [
            'text contain stop word',
            'text contains uppercasing',
            'text contain high profile punctuation',
            'text contain multiple space need remove',
            'text contain word require lemmatisation play processor class',
            'text contain contraction shall alicia think',
        ]

        preprocessor = preprocessing.BagOfWordsPreprocessor()

        assert preprocessor._preprocess_texts(input_texts) == expected_output
