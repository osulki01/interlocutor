"""Encode/embed text so it is represented in a form which can be used by machine learning algorithms."""

# Standard libaries
from typing import Dict, List

# Third party libraries
import pandas as pd
from psycopg2 import sql as psy_sql
from sklearn.feature_extraction import text as sklearn_text

# Internal imports
from interlocutor.database import postgresql


class TfidfEncoder:
    """Represent texts with a tf-idf transformed matrix."""

    def __init__(self, use_existing_vocab: bool = True):
        """
        Initialise attributes of class.

        Parameters
        ----------
        use_existing_vocab : bool (default False)
            Whether to re-fit the vectoriser using all of the article texts (False) or to use an existing vocabulary
            produced by a previous run (True, default).
        """

        self._db_connection = postgresql.DatabaseConnection()
        self._use_existing_vocab = use_existing_vocab

    def _analyse_and_overwrite_existing_vocabulary(self, preprocessed_content: List[str]) -> None:
        """
        Capture all of the distinct words appearing across the preprocessed version of articles and save to database.

        Parameters
        ----------
        preprocessed_content : list[str]
            Preprocessed version of content for each article.
        """

        vectoriser = sklearn_text.TfidfVectorizer()
        vectoriser.fit(preprocessed_content)

        # Extract all of the unique words found
        words = vectoriser.get_feature_names()
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

    def encode_articles(self) -> None:
        """
        Represent articles as tf-idf matrix and save to database. Only runs on articles which have not already been
        encoded if using an existing vocabulary, otherwise will fit and re-encode all articles.
        """

        # Retrieve all of the preprocessed version of articles
        preprocessed_content = self._load_all_articles_bow_preprocessed_content()

        # If a new vocabulary needs to be established, then analyse all texts
        if not self._use_existing_vocab:
            self._analyse_and_overwrite_existing_vocabulary(preprocessed_content['processed_content'].values)

        # Create a vectoriser using either the pre-existing vocabulary or a new one which has been extracted
        vocabulary = self._load_vocabulary()
        vectoriser = sklearn_text.TfidfVectorizer(vocabulary=vocabulary)

        encoded_articles_matrix = vectoriser.fit_transform(preprocessed_content['processed_content'].values)

        encoded_articles_dataframe = pd.DataFrame(
            columns=list(vocabulary.keys()),
            index=preprocessed_content['id'].values,
            data=encoded_articles_matrix.toarray()
        )

        encoded_articles_dataframe = encoded_articles_dataframe.reset_index().rename(columns={'index': 'id'})

        # Fully replace tf-idf table if vocabulary has been built again from scratch and the dimensions of the matrix
        # will have changed
        if_exists = 'append' if self._use_existing_vocab else 'replace'

        self._db_connection.upload_dataframe(
            dataframe=encoded_articles_dataframe,
            table_name='tfidf_representation',
            schema='encoded_articles',
            if_exists=if_exists,
            index=False
        )

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

            # If using an existing vocabulary, only pull the articles which have not yet been encoded
            if self._use_existing_vocab:

                sql_query = psy_sql.SQL("""
                    SELECT * FROM {source_schema_and_table} 
                    WHERE id NOT IN (SELECT id FROM encoded_articles.tfidf_representation);
                    """).format(
                    source_schema_and_table=psy_sql.Identifier(publication, 'article_content_bow_preprocessed')
                )

                publication_content = self._db_connection.get_dataframe(query=sql_query)

            # Otherwise re-load all articles to encode again
            else:
                publication_content = self._db_connection.get_dataframe(
                    table_name='article_content_bow_preprocessed',
                    schema=publication
                )

            all_preprocessed_content = pd.concat([all_preprocessed_content, publication_content])

        return all_preprocessed_content

    def _load_vocabulary(self) -> Dict[str, int]:
        """
        Load existing vocabulary from database gathered from previous tf-idf encoding of all articles.

        Returns
        -------
        dict[str, int]
            Mapping of word and its corresponding index in the tf-idf feature matrix.
        """

        df_existing_vocab = self._db_connection.get_dataframe(table_name='tfidf_vocabulary', schema='encoded_articles')

        df_existing_vocab.set_index('word', inplace=True)

        return df_existing_vocab['feature_matrix_index'].to_dict()
