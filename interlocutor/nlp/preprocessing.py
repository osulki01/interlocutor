"""Tidying and preprocessing text data."""

# Standard library imports
import collections
from typing import List

# Third party imports
from psycopg2 import sql as psy_sql
import spacy
import tqdm

# Internal imports
from interlocutor.database import postgresql


class BagOfWordsPreprocessor:
    """
    Preprocessor of text for simpler NLP tasks like tf-idf.
    """

    def __init__(
            self,
            batch_size: int = 1,
            number_of_processors: int = 1,
    ):
        """
        Initialise attributes of class.

        Parameters
        ----------
        batch_size : int (default 1)
            The number of texts to process at one time.

        number_of_processors : int (default 1)
            Number of processors used to to process texts in parallel. If set to -1, it will use all available CPUs
            (equivalent of `multiprocessing.cpu_count()`.
        """

        self._batch_size = batch_size
        self._number_of_processors = number_of_processors
        self._spacy_nlp = spacy.load(name='en_core_web_sm', disable=['ner', 'parser', 'tagger', 'textcat'])
        self._db_connection = postgresql.DatabaseConnection()

        self._daily_mail_db = {
            'schema': 'daily_mail',
            'raw_content': 'recent_article_content',
            'processed_content': 'recent_article_content_bow_processed'
        }

        self._guardian_db = {
            'schema': 'the_guardian',
            'raw_content': 'article_content',
            'processed_content': 'article_content_bow_processed'
        }

    def preprocess_all_article_content(self) -> None:
        """
        Extract all of the content from articles (which have not already been preprocessed), transform the text,
        and then upload to database.
        """

        for schema in ['daily_mail', 'the_guardian']:

            print(f'Preprocessing articles from {schema}')

            # Extract articles which have not already been processed
            sql_query = psy_sql.SQL(
                string="""SELECT id, content
                          FROM {raw_content}
                          WHERE id NOT IN (SELECT ID FROM {processed_content})
                          """).format(
                raw_content=psy_sql.Identifier(schema, 'article_content'),
                processed_content=psy_sql.Identifier(schema, 'article_content_bow_preprocessed')
            )

            articles = self._db_connection.get_dataframe(query=sql_query)

            articles['processed_content'] = self._preprocess_texts(articles['content'].values)
            articles.drop(columns='content', inplace=True)  # Only retain processed content

            self._db_connection.upload_new_data_only_to_existing_table(
                dataframe=articles,
                table_name='article_content_bow_preprocessed',
                schema=schema,
                id_column='id'
            )

    def _preprocess_texts(self, texts: List[str]) -> List[str]:
        """
        Remove stop words and punctuation, then lemmatise and make everything lowercase.

        Returns
        -------
        list[str]
            List of preprocessed version of all the texts.
        """

        transformed_texts = collections.deque()

        for document in tqdm.tqdm(
                desc='Documents processed',
                iterable=self._spacy_nlp.pipe(
                    texts=texts,
                    batch_size=self._batch_size,
                    n_process=self._number_of_processors
                ),
                total=len(texts),
                unit=' document'
        ):
            processed_doc = [
                token.lemma_.lower() for token in document if not self._token_should_be_deleted(token)
            ]

            processed_doc = ' '.join(processed_doc)

            transformed_texts.append(processed_doc)

        return list(transformed_texts)

    @staticmethod
    def _token_should_be_deleted(token: spacy.tokens.Token) -> bool:
        """
        Check whether the token matches any of the criteria suggesting it should be deleted e.g. it is a stop word,
        punctuation, or whitespace.

        Parameters
        ----------
        token : spacy.tokens.Token
            Individual token (can be a word, character, or sub-word) that should be checked.

        Returns
        -------
        bool
            True if the token should be deleted, False otherwise.
        """

        return bool(token.is_punct or token.is_space or token.is_stop)


if __name__ == '__main__':

    print('Initialising class for preprocessing text in preparation for bag of words algorithms')
    bow_preprocessor = BagOfWordsPreprocessor(batch_size=5, number_of_processors=-1)

    print('Preprocess all articles')
    bow_preprocessor.preprocess_all_article_content()
