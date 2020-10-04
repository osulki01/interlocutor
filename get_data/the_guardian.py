# Standard libraries
import functools
import time
import os
from typing import Any, Callable, Dict, Union

# Third party libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import tqdm


def retry(total_attempts) -> Callable:
    """
    Execute the decorated function which calls an API using the requests function, and retry a specified number of
    times if it encounters an exception (assumes that the function which makes the request explicitly raises an
    exception).

    Parameters
    ----------
    total_attempts : int
        Number of times that the decorated function should attempt to be executed.

    Returns
    -------
    Callable
        Decorated version of function which will retry the requested number of times if it encounters an error.

    Raises
    ------
    requests.exceptions.RequestException
        If maximum number of request attempts reached without success.
    """

    def retry_decorator(func):

        @functools.wraps(func)
        def func_with_retries(*args, **kwargs):

            attempt_number = 1

            while attempt_number <= total_attempts:
                try:
                    return func(*args, **kwargs)

                except requests.RequestException as request_exception:

                    print(f'Function {func.__name__} failed on attempt {attempt_number} of {total_attempts} total '
                          f'attempts.')

                    if attempt_number == 3:
                        print('Max attempts reached. Stopping now.')
                        raise request_exception

                    attempt_number += 1
                    print('Retrying now.')

        return func_with_retries
    return retry_decorator


class ArticleDownloader:
    """
    Call The Guardian API to capture information about the articles in its Opinion section.
    """

    def __init__(
            self,
            article_contents_file: str = 'data/the_guardian/opinion_articles_contents.csv',
            metadata_file: str = 'data/the_guardian/opinion_articles_metadata.csv'
    ):
        """
        Parameters
        ----------
        article_contents_file : str (default 'data/the_guardian/opinion_articles_contents.csv')
            File name/path which contains the text content of the articles.
        metadata_file : str (default 'data/the_guardian/opinion_articles_contents.csv')
            File name/path which contains metadata about the articles which have already been stored.
        """

        self._api_key = os.getenv('GUARDIAN_API_KEY')
        self._article_contents_file = article_contents_file
        self._metadata_file = metadata_file
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

    def _get_latest_opinion_articles_datetime_reached(self) -> Union[str, None]:
        """
        Get the latest publication date reached by previous efforts to extract all articles from the Opinion section.

        Returns
        -------
        str or None
            Most recent publication date of the articles that have already had their metadata pulled
            e.g. '2020-08-25T06:00:46Z'. None if no article metadata have been saved to disk already.
        """

        if os.path.isfile(self._metadata_file):
            existing_article_metadata = pd.read_csv(self._metadata_file)
            latest_web_publication_datetime = existing_article_metadata['webPublicationDate'].max()
        else:
            latest_web_publication_datetime = None

        return latest_web_publication_datetime

    # The Guardian API only allows you to progress through a certain number of pages, so retry and pick up from latest
    # article reached if the method hits an error.
    @retry(total_attempts=3)
    def record_opinion_articles(self) -> None:
        """
        Save a dataframe to disk storing all of articles appearing in The Guardian Opinion section
        (https://www.theguardian.com/uk/commentisfree) and how they can be accessed via the API.

        Dataframe contains one row per article in The Guardian Opinion section containing metadata about the article.
        """

        # The free Guardian API key only allows 500 calls per day, but only a certain number of articles can be pulled
        # with each call (max 200 articles), so calculate how many pages have to be called to cover all articles
        page_size = 200

        most_recent_datetime = self._get_latest_opinion_articles_datetime_reached()

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
                time.sleep(2)

            # Break the loop if an error is encountered, but save the progress made
            except requests.exceptions.RequestException as request_error:
                print(f'Error making API request on Page {page_index} of {total_pages}')
                print(f'Exception: {request_error}')

                if opinion_articles_metadata_per_api_call is not None:
                    print(f'\nSaving metadata already pulled to {self._metadata_file}')
                    all_opinion_articles = pd.concat(opinion_articles_metadata_per_api_call)
                    self._save_opinion_article_metadata_to_disk(all_opinion_articles)

                else:
                    print('No article metadata pulled.')

                raise request_error

        print(f'\nAll articles processed, saving metadata to {self._metadata_file}')
        all_opinion_articles = pd.concat(opinion_articles_metadata_per_api_call)
        self._save_opinion_article_metadata_to_disk(all_opinion_articles)

    def _save_opinion_article_metadata_to_disk(self, metadata: pd.DataFrame) -> None:
        """
        Save the articles metadata to a csv file (and append if it already exists).

        Parameters
        ----------
        metadata : pandas.DataFrame
            Data describing the articles that have already been recorded.
        """

        if os.path.isfile(self._metadata_file):
            write_mode = 'a'
            header = False
        else:
            write_mode = 'w'
            header = True

        metadata.to_csv(path_or_buf=self._metadata_file, header=header, index=False, mode=write_mode)
