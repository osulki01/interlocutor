"""Interact with The Guardian API and download article metadata/content."""

# Standard libraries
import hashlib
import os
import time
from typing import Any, Dict, List, Union

# Third party libraries
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import requests
import tqdm

# Internal imports
from interlocutor.commons import commons
from interlocutor.database import postgresql


class ArticleDownloader:
    """
    Call The Guardian API to capture information about the articles in its Opinion section.
    """

    def __init__(self):

        self._api_key = os.getenv('GUARDIAN_API_KEY')
        self._db_connection = postgresql.DatabaseConnection()
        self._opinion_section_url = 'https://content.guardianapis.com/commentisfree/commentisfree'

    def _call_api_and_display_exceptions(self, url: str, params: dict = None) -> Dict[str, Any]:
        """
        Make call to Guardian API using authentication key from .env file and display any errors which occur.

        Parameters
        ----------
        url : str
            URL of the Guardian API which a get request will be sent to
            e.g. 'https://content.guardianapis.com/commentisfree/2020/sep/22/starmer-labour-leader-brexit-johnson'

        params : dict (optional)
            Parameters submitted to the server when making the API request (no need to include api-key as this is
            already accounted for)
            e.g. {'format': json, 'query-fields': ['body', 'thumbnail']}

            Appropriate parameters for the API call can be found here:
            https://open-platform.theguardian.com/documentation/

        Returns
        -------
        Dict[str, Any]
            Dictionary version of the API response object.

        Raises
        ------
        requests.exceptions.RequestException
            If request does not succeed.
        """

        # Use api-key from environment variable and assume a json should be returned, but allow this payload to be
        # overwritten/appended to based on user-specified values
        payload = {'api-key': self._api_key, 'format': 'json'}

        if params:
            for parameter in params:
                payload[parameter] = params[parameter]

        # Call API but capture any exceptions, which can sometimes be masked by requests library otherwise
        try:
            api_response = requests.get(url=url, params=payload)
            api_response.raise_for_status()

            return api_response.json()

        except requests.exceptions.RequestException as request_error:
            print(f'Error calling Guardian API: {request_error}')
            raise request_error

    def _get_article_content(self, article_api_url: str) -> str:
        """
        Retrieve the text content of an article.

        Parameters
        ----------
        article_api_url : str
            The URL for the raw content of an article
            e.g. 'https://content.guardianapis.com/commentisfree/2020/sep/22/starmer-labour-leader-brexit-johnson'

        Returns
        -------
        str
            Raw text content of the article.
        """

        api_response = self._call_api_and_display_exceptions(url=article_api_url, params={'show-fields': 'body'})

        article_content_html = api_response['response']['content']['fields']['body']

        soup = BeautifulSoup(markup=article_content_html, features='html.parser')
        article_content_text = soup.get_text(separator=" ", strip=True)

        return article_content_text

    def _get_latest_opinion_articles_datetime_reached(self, data_type: str) -> Union[str, None]:
        """
        Get the latest publication date reached by previous efforts to extract all articles from the Opinion section.

        Parameters
        ----------
        data_type : str (either 'metadata' or 'content')
            Whether to check the most recent article reached in terms of collecting its metadata or actual content.

        Returns
        -------
        str or None
            Most recent publication date of the articles that have already had their metadata pulled
            e.g. '2020-08-25T06:00:46Z'. None if no article metadata have been saved to the database already.
        """

        if data_type == 'metadata':
            table_to_query = 'article_metadata'
        elif data_type == 'content':
            table_to_query = 'article_content'
        else:
            raise ValueError("`data_type` must either be 'metadata' or 'content'")

        most_recent = self._db_connection.get_min_or_max_from_column(
            table_name=table_to_query,
            schema='the_guardian',
            min_or_max='max',
            column='web_publication_timestamp'
        )

        if most_recent is not None:
            most_recent = pd.to_datetime(most_recent).strftime('%Y-%m-%dT%H:%M:%SZ')

        return most_recent

    def record_opinion_articles_content(self, number_of_articles: int = 100) -> None:
        """
        Save a dataframe to disk storing the content of of articles appearing in The Guardian Opinion section
        (https://www.theguardian.com/uk/commentisfree).

        Storing all of the text can be expensive so iterate through a specified number of articles, working from the
        most recently published articles backwards.

        Dataframe contains one row per article in The Guardian Opinion section that has already been crawled to extract
        its metadata.

        Parameters
        ----------
        number_of_articles : int (default 100)
            Number of articles to iterate through and extract their contents.
        """

        if os.path.isfile(self._article_contents_file):
            articles_to_crawl = pd.read_csv(filepath_or_buffer=self._article_contents_file)

            # Duplicate articles may appear from originally the metadata caller as it picks up from the latest
            # article data already retrieved each time it retries
            articles_to_crawl.drop_duplicates(subset='id', inplace=True)
            articles_to_crawl.set_index('id', inplace=True)

        else:
            articles_to_crawl = pd.read_csv(
                filepath_or_buffer=self._metadata_file,
                usecols=['id', 'apiUrl', 'webPublicationDate'],
                index_col='id'
            )

            articles_to_crawl['content'] = np.nan
            articles_to_crawl.sort_values(by='webPublicationDate', ascending=False, inplace=True)

        next_article_to_pull_index = np.argmax(articles_to_crawl['content'].isna())
        remaining_articles = articles_to_crawl.index[
                             next_article_to_pull_index:(next_article_to_pull_index + number_of_articles)
                             ]

        counter = 0

        for article_id in tqdm.tqdm(
                desc='Article content retrieved',
                iterable=remaining_articles,
                total=len(remaining_articles),
                unit=' article'
        ):

            article_api_url = articles_to_crawl.loc[article_id, 'apiUrl']

            try:
                article_content = self._get_article_content(article_api_url)
                articles_to_crawl.loc[article_id, 'content'] = article_content

                counter += 1

            except requests.exceptions.RequestException as request_exception:
                print(f'Error retrieving contents for article {article_api_url}')
                print(f'Exception: {request_exception}')
                print(f'\nSaving article content already pulled to {self._article_contents_file}')

                articles_to_crawl.to_csv(self._article_contents_file)
                raise request_exception

            # Be polite, do not bombard API with too many requests at once
            time.sleep(0.5)

            if counter % 50 == 0:

                print(f'Checkpoint, article {counter} reached.')
                print(f'\nSaving article content already pulled to {self._article_contents_file}')
                articles_to_crawl.to_csv(self._article_contents_file)

        print(f'\nSaving article content to {self._article_contents_file}')
        articles_to_crawl.to_csv(self._article_contents_file)

    # The Guardian API only allows you to progress through a certain number of pages, so retry and pick up from latest
    # article reached if the method hits an HTTP error.
    @commons.retry(total_attempts=5, exceptions_to_check=requests.exceptions.HTTPError)
    def record_opinion_articles_metadata(self) -> None:
        """
        Save a dataframe to disk storing all of articles appearing in The Guardian Opinion section
        (https://www.theguardian.com/uk/commentisfree) and how they can be accessed via the API.

        Dataframe contains one row per article in The Guardian Opinion section detailing metadata about the article.
        """

        # Only a certain number of articles can be pulled with each call (max 200 articles), so calculate how many
        # pages have to be called to cover all articles
        page_size = 200

        most_recent_datetime = self._get_latest_opinion_articles_datetime_reached(data_type='metadata')

        if most_recent_datetime:
            print(f'Articles published on or after {most_recent_datetime} will be processed.')

        opinion_section_metadata = self._call_api_and_display_exceptions(
            url=self._opinion_section_url,
            params={'page-size': page_size, 'from-date': most_recent_datetime}
        )
        total_pages = opinion_section_metadata['response']['pages']

        # Call API to record remaining articles
        opinion_articles_metadata_per_api_call = []

        for page_index in tqdm.tqdm(
                desc='API pages processed',
                iterable=range(1, (total_pages + 1)),
                total=total_pages,
                unit=' page'
        ):

            try:
                opinion_articles_metadata_json = self._call_api_and_display_exceptions(
                    url=self._opinion_section_url,
                    params={
                        'page': page_index,
                        'page-size': page_size,
                        'order-by': 'oldest',
                        'from-date': most_recent_datetime
                    }
                )

                opinion_articles_metadata_df = pd.DataFrame.from_dict(
                    data=opinion_articles_metadata_json['response']['results']
                )

                opinion_articles_metadata_per_api_call.append(opinion_articles_metadata_df)

                # Be polite, do not bombard API with too many requests at once
                time.sleep(0.5)

            # Break the loop if an error is encountered, but save the progress made
            except requests.exceptions.RequestException as request_error:
                print(f'Error making API request on Page {page_index} of {total_pages}')
                print(f'Exception: {request_error}')
                print('\nSaving metadata already pulled to the_guardian.metadata postgres table.')
                self._write_metadata_to_postgres(metadata_per_api_call=opinion_articles_metadata_per_api_call)

                raise request_error

        print('\nAll articles processed, saving data to the_guardian.metadata postgres table.')
        self._write_metadata_to_postgres(metadata_per_api_call=opinion_articles_metadata_per_api_call)

    def _write_metadata_to_postgres(self, metadata_per_api_call: List[pd.DataFrame]) -> None:
        """
        Prepare the data gathered from each API call and write to postgres.

        Parameters
        ----------
        metadata_per_api_call : list
            List of dataframes, each containing the metadata collected per API call.
        """

        if len(metadata_per_api_call) == 0:
            print('No article metadata pulled.')
            return

        all_opinion_articles = pd.concat(metadata_per_api_call)

        all_opinion_articles.drop(columns='isHosted', inplace=True)

        all_opinion_articles.rename(
            columns={
                'id': 'guardian_id',
                'type': 'content_type',
                'sectionId': 'section_id',
                'sectionName': 'section_name',
                'webPublicationDate': 'web_publication_timestamp',
                'webTitle': 'web_title',
                'webUrl': 'web_url',
                'apiUrl': 'api_url',
                'pillarId': 'pillar_id',
                'pillarName': 'pillar_name'
            },
            inplace=True
        )

        all_opinion_articles['web_publication_timestamp'] = pd.to_datetime(
            all_opinion_articles['web_publication_timestamp'],
            format='%Y-%m-%dT%H:%M:%SZ'
        )

        all_opinion_articles['id'] = [
            hashlib.md5(val.encode('utf-8')).hexdigest() for val in all_opinion_articles['guardian_id']
        ]

        self._db_connection.upload_new_data_only_to_existing_table(
            dataframe=all_opinion_articles,
            table_name='article_metadata',
            schema='the_guardian',
            id_column='id'
        )
