# Standard libraries
import time
import os
from typing import Any, Dict

# Third party libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import tqdm


def _call_api_and_handle_errors(url: str, params: dict = None) -> Dict[str, Any]:
    """
    Make call to Guardian API using authentication key from .env file and display any errors which occur.

    Parameters
    ----------
    url: str
        URL of the Guardian API which a get request will be sent to
        e.g. 'https://content.guardianapis.com/commentisfree/2020/sep/22/starmer-labour-leader-brexit-johnson'

    params : dict (optional)
        Parameters submitted to the server when making the API request (no need to include api-key as this is already
        accounted for)
        e.g. {'format': json, 'query-fields': ['body', 'thumbnail']}

        Appropriate parameters for the API call can be found here: https://open-platform.theguardian.com/documentation/

    Returns
    -------
    Dict[str, Any]
        Dictionary version of the API response object.
    """

    # Use api-key from environment variable and assume a json should be returned, but allow this payload to be
    # overwritten/appended to based on user-specified values
    payload = {'api-key': os.getenv('GUARDIAN_API_KEY'), 'format': 'json'}

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


def get_article_content(article_api_url: str) -> str:
    """

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

    api_response = _call_api_and_handle_errors(url=article_api_url, params={'show-fields': 'body,wordcount'})

    print(api_response)

    article_content_html = api_response['response']['content']['fields']['body']

    soup = BeautifulSoup(markup=article_content_html, features='html.parser')
    article_content_text = soup.get_text(separator=" ", strip=True)

    return article_content_text


def _get_latest_opinion_articles_page_reached(file_name_or_path: str) -> int:
    """
    The latest page index reached by previous efforts to extract all artucles from the Opinion section.

    Parameters
    ----------
    file_name_or_path : str
        File which contains how many API pages' metadata has been stored already.

    Returns
    -------
    int
        Page index which had been reached (1 if no history of previous attempts exists).
    """

    if os.path.isfile(file_name_or_path):
        with open(file=file_name_or_path, mode='rt') as opened_file:
            contents_of_file = opened_file.read()
            page_index_reached = int(contents_of_file.split(' ')[0])
    else:
        page_index_reached = 1

    return page_index_reached


def record_opinion_articles():
    """
    Save a dataframe to disk storing all of articles appearing in The Guardian Opinion section
    (https://www.theguardian.com/uk/commentisfree) and how they can be accessed via the API.

    Dataframe contains one row per article in The Guardian Opinion section containing metadata about the article, and is
    saved to 'data/the_guardian/opinion_articles_metadata.csv'
    """

    opinion_section_url = 'https://content.guardianapis.com/commentisfree/commentisfree'

    # The free Guardian API key only allows 500 calls per day, but only a certain number of articles can be pulled with
    # each call (max 200 articles), so calculate how many pages have to be called to cover all articles
    page_size = 200
    opinion_section_metadata = _call_api_and_handle_errors(url=opinion_section_url, params={'page-size': page_size})
    total_pages = opinion_section_metadata['response']['pages']

    progress_file = 'data/the_guardian/opinion_articles_page_index_reached.txt'
    page_index_already_reached = _get_latest_opinion_articles_page_reached(file_name_or_path=progress_file)
    pages_left = range(page_index_already_reached, (total_pages + 1))

    # Call API to record remaining articles
    opinion_articles_metadata_per_api_call = []

    for page_index in tqdm.tqdm(
            desc='API pages processed',
            iterable=pages_left,
            total=len(pages_left),
            unit=' page'
    ):

        try:
            opinion_articles_metdata_json = _call_api_and_handle_errors(
                url=opinion_section_url,
                params={'page': page_index, 'page-size': page_size, 'orderBy': 'oldest'}
            )

            opinion_articles_metadata_df = pd.DataFrame.from_dict(opinion_articles_metdata_json['response']['results'])

            opinion_articles_metadata_per_api_call.append(opinion_articles_metadata_df)

            # Be polite, do not bombard API with too many requests at once
            time.sleep(2)

        except requests.exceptions.RequestException as request_error:
            print(f'Error making API request on Page {page_index} of {total_pages}')
            print('Possibly hit limit of API calls for the day')
            print(f'Exception: {request_error}')

            # Record how much progress was made
            with open(file=progress_file, mode='wt') as opened_file:
                opened_file.write(f'{page_index} of {total_pages} processed already')

            break

    all_opinion_articles = pd.concat(opinion_articles_metadata_per_api_call)

    # Save the metadata already gathered to disk
    opinion_article_metadata_file = 'data/the_guardian/opinion_articles_metadata.csv'
    if os.path.isfile(opinion_article_metadata_file):
        write_mode = 'a'
        header = False
    else:
        write_mode = 'w'
        header = True

    all_opinion_articles.reset_index().to_csv(
        path_or_buf=opinion_article_metadata_file,
        mode=write_mode,
        header=header
    )
