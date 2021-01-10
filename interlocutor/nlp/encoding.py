"""Encode/embed text so it is represented in a form which can be used by machine learning algorithms."""

# Third party libraries
import pandas as pd
from sklearn.feature_extraction import text as sklearn_text

# Internal imports
from interlocutor.database import postgresql


class TfidfEncoder:
    """Represent texts with a tf-idf transformed matrix."""

    def __init__(self, use_existing_vocab: bool = False):
        """
        Initialise attributes of class.

        Parameters
        ----------
        use_existing_vocab : bool (default False)
            Whether to retrain the vectoriser using all of the article texts or to use an existing vocabulary produced
            by a previous run.
        """

        self._db_connection = postgresql.DatabaseConnection()
        self._preprocessed_content = None
        self._vectoriser = None
        self._vocabulary = self._load_existing_vocabulary() if use_existing_vocab else None

    def _analyse_and_overwrite_existing_vocabulary(self) -> None:
        """
        Capture all of the distinct words appearing across the preprocessed version of articles and save to database.
        """

        # Load content and fit a tfidf vectoriser on it
        self._preprocessed_content = self._load_all_articles_bow_preprocessed_content()
        self._create_vectoriser()
        self._vectoriser.fit(self._preprocessed_content['processed_content'].values)

        # Extract all of the unique words found
        words = self._vectoriser.get_feature_names()
        new_vocabulary = pd.DataFrame(data={'word': words, 'feature_matrix_index': range(len(words))})

        # Replace existing data
        self._db_connection.execute_database_operation("TRUNCATE TABLE encoded_articles.tfidf_vocabulary;")

        self._db_connection.upload_dataframe(
            dataframe=new_vocabulary,
            table_name='tfidf_vocabulary',
            schema='encoded_articles',
            if_exists='append',
            index=False,
        )

    def _create_vectoriser(self):
        """Establish a vectoriser depending and account for whether an existing vocabulary exists or not."""

        self._vectoriser = sklearn_text.TfidfVectorizer(vocabulary=self._vocabulary)

    def _load_all_articles_bow_preprocessed_content(self) -> pd.DataFrame:
        """
        Read the bag of words preprocessed content from all of the articles available.

        Returns
        -------
        pandas.DataFrame
            ID for all articles alongside a preprocessed version of their text content.
        """

        # Placeholder dataframe to store the article content
        all_preprocessed_content = pd.DataFrame(columns=['id', 'processed_content'])

        # Append preprocessed content from each publication
        for publication in ['daily_mail', 'the_guardian']:
            publication_content = self._db_connection.get_dataframe(
                table_name='article_content_bow_preprocessed',
                schema=publication
            )

            all_preprocessed_content = pd.concat([all_preprocessed_content, publication_content])

        return all_preprocessed_content

    def _load_existing_vocabulary(self) -> pd.DataFrame:
        """
        Load existing vocabulary from database gathered from previous tf-idf encoding of all articles.
        """

        df_existing_vocab = self._db_connection.get_dataframe(table_name='tfidf_vocabulary', schema='encoded_articles')

        df_existing_vocab.set_index('word', inplace=True)

        return df_existing_vocab['feature_matrix_index'].to_dict()
