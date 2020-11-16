"""Testing interaction with The Guardian API and downloading of article metadata/content."""

# Standard libraries
import datetime
from typing import Any, Dict

# Third party libraries
import numpy as np
import pandas as pd
import pytest
import requests
import time

# Internal imports
from interlocutor.get_data import the_guardian
from interlocutor.database import postgresql


def test_call_api_and_display_exceptions_raises_exception():
    """Exception is raised and shown when API request fails."""

    article_downloader = the_guardian.ArticleDownloader()

    url = 'http://content.guardianapis.com/tags/INCORRECT_URL'

    with pytest.raises(requests.exceptions.RequestException):
        article_downloader._call_api_and_display_exceptions(url=url)


def test_call_api_and_display_exceptions_makes_successful_call():
    """Calls can be successfully made against API."""

    article_downloader = the_guardian.ArticleDownloader()

    url = 'https://content.guardianapis.com/editions'
    params = {'q': 'uk'}

    expected_response = {
        'response': {
            'status': 'ok',
            'userTier': 'developer',
            'total': 1,
            'results': [
                {
                    'id': 'uk',
                    'path': 'uk',
                    'edition': 'UK',
                    'webTitle': 'new guardian uk front page',
                    'webUrl': 'https://www.theguardian.com/uk',
                    'apiUrl': 'https://content.guardianapis.com/uk'
                }
            ]
        }
    }

    actual_response = article_downloader._call_api_and_display_exceptions(url=url, params=params)

    assert actual_response == expected_response


def test_get_article_content(monkeypatch):
    """Text content of articles are correctly retrieved as a string."""

    def mock_api_call(url: str, params: dict) -> Dict[str, Any]:
        """Mock functionality of making Guardian API call."""
        return {
            'response': {
                'status': 'ok',
                'userTier': 'developer',
                'total': 1,
                'content': {
                    'id': 'world/2002/feb/25/race.uk',
                    'type': 'article',
                    'sectionId': 'commentisfree',
                    'sectionName': 'Opinion',
                    'webPublicationDate': '2002-02-25T01:53:00Z',
                    'webTitle': 'Gary Younge: Terms of abuse',
                    'webUrl': 'https://www.theguardian.com/world/2002/feb/25/race.uk',
                    'apiUrl': 'https://content.guardianapis.com/world/2002/feb/25/race.uk',
                    'fields': {
                        'body': '<p>About every three months I am accused of being an anti-semite. It is not '
                                'difficult to predict when it will happen. A single, critical mention of Israel\'s '
                                'treatment of Palestinians will do it, as will an article that does not portray Louis '
                                'Farrakhan as Satan\'s representative on earth. Of the many and varied responses I '
                                'get to my work - that it is anti-white (insane), anti-American (inane) and '
                                'anti-Welsh (intriguing) - anti-semitism is one charge that I take more seriously '
                                'than most. </p> <p>Such engagement will not be easy, for the semantic differences '
                                'reflect fundamental disagreements. But if it cannot be achieved in Britain, what hope '
                                'is there for the Middle East? </p> '
                                '<p><ahref="mailto:g.younge@theguardian.com">g.younge@theguardian.com</a></p> '
                    },
                    'isHosted': False,
                    'pillarId': 'pillar/opinion',
                    'pillarName': 'Opinion'
                }
            }
        }

    article_downloader = the_guardian.ArticleDownloader()
    monkeypatch.setattr(article_downloader, "_call_api_and_display_exceptions", mock_api_call)

    expected_article_content = ('About every three months I am accused of being an anti-semite. It is not '
                                'difficult to predict when it will happen. A single, critical mention of '
                                "Israel's treatment of Palestinians will do it, as will an article that does "
                                "not portray Louis Farrakhan as Satan's representative on earth. Of the many "
                                'and varied responses I get to my work - that it is anti-white (insane), '
                                'anti-American (inane) and anti-Welsh (intriguing) - anti-semitism is one '
                                'charge that I take more seriously than most. Such engagement will not be '
                                'easy, for the semantic differences reflect fundamental disagreements. But if '
                                'it cannot be achieved in Britain, what hope is there for the Middle East? '
                                'g.younge@theguardian.com')

    actual_article_content = article_downloader._get_article_content(article_api_url='mock_url')

    assert actual_article_content == expected_article_content


def test_get_latest_opinion_articles_datetime_reached():
    """
    Correct marker of progress made by previous API calls is retrieved by showing the latest publication datetime
    reached.
    """

    # Metadata table
    expected_metadata = '2002-02-25T01:53:00Z'

    article_downloader = the_guardian.ArticleDownloader()

    actual_metadata = article_downloader._get_latest_opinion_articles_datetime_reached(data_type='metadata')

    assert actual_metadata == expected_metadata

    # Content table
    expected_content = '2002-02-25T01:53:00Z'

    actual_content = article_downloader._get_latest_opinion_articles_datetime_reached(data_type='content')

    assert actual_content == expected_content


# "fs" is the reference to the fake file system from the fixture provided by pyfakefs library
@pytest.mark.skip(reason="Changing approach to use database rather than local CSVs")
def test_record_opinion_articles_content(fs, monkeypatch):
    """
    The content of articles are pulled and saved to disk, and the script can pick up from where it last finished.
    """

    # Create file containing article metadata
    mock_metadata_file = 'mock_metadata_file.csv'
    mock_metadata_dict = {
        'id': ['politics/1990/nov/23/past.conservatives', 'world/2002/feb/25/race.uk'],
        'type': ['article', 'article'],
        'sectionId': ['commentisfree', 'commentisfree'],
        'sectionName': ['Opinion', 'Opinion'],
        'webPublicationDate': ['1990-11-23T16:47:00Z', '2002-02-25T01:53:00Z'],
        'webTitle': ['The Thatcher Years | Hugo Young', 'Gary Younge: Terms of abuse'],
        'webUrl': ['https://www.theguardian.com/politics/1990/nov/23/past.conservatives',
                   'https://www.theguardian.com/world/2002/feb/25/race.uk'],
        'apiUrl': ['https://content.guardianapis.com/politics/1990/nov/23/past.conservatives',
                   'https://content.guardianapis.com/world/2002/feb/25/race.uk'],
        'isHosted': ['FALSE', 'FALSE'],
        'pillarId': ['pillar/opinion', 'pillar/opinion'],
        'pillarName': ['Opinion', 'Opinion'],
    }

    mock_metadata_df = pd.DataFrame(data=mock_metadata_dict)
    mock_metadata_df.to_csv(mock_metadata_file)

    # Avoid calling API or using time.sleep in test
    def mock_article_content_call(article_api_url: str) -> str:
        """Mock functionality of class method which extracts content of articles."""

        return "Some article content."

    mock_article_contents_file = 'contents_file.csv'

    article_downloader = the_guardian.ArticleDownloader(
        metadata_file=mock_metadata_file,
        article_contents_file=mock_article_contents_file
    )
    monkeypatch.setattr(article_downloader, "_get_article_content", mock_article_content_call)
    monkeypatch.setattr(time, 'sleep', lambda s: None)

    ######################################################################
    # Scenario 1: No article contents have already been extracted
    ######################################################################

    article_downloader.record_opinion_articles_content(number_of_articles=1)

    # Expected data should have been processed from most recent publication backwards
    expected_article_contents_first_time = pd.DataFrame(
        data={
            'id': ['world/2002/feb/25/race.uk', 'politics/1990/nov/23/past.conservatives'],
            'webPublicationDate': ['2002-02-25T01:53:00Z', '1990-11-23T16:47:00Z'],
            'apiUrl': ['https://content.guardianapis.com/world/2002/feb/25/race.uk',
                       'https://content.guardianapis.com/politics/1990/nov/23/past.conservatives'],
            'content': ['Some article content.', np.nan]
        }
    )

    actual_article_contents_first_time = pd.read_csv(mock_article_contents_file)

    pd.testing.assert_frame_equal(actual_article_contents_first_time, expected_article_contents_first_time)

    ######################################################################
    # Scenario 2: Some of the articles have already been extracted
    ######################################################################

    # The content for the second article should now be available
    expected_article_contents_second_time = pd.DataFrame(
        data={
            'id': ['world/2002/feb/25/race.uk', 'politics/1990/nov/23/past.conservatives'],
            'webPublicationDate': ['2002-02-25T01:53:00Z', '1990-11-23T16:47:00Z'],
            'apiUrl': ['https://content.guardianapis.com/world/2002/feb/25/race.uk',
                       'https://content.guardianapis.com/politics/1990/nov/23/past.conservatives'],
            'content': ['Some article content.', 'Some article content.']
        }
    )

    article_downloader.record_opinion_articles_content(number_of_articles=1)
    actual_article_contents_second_time = pd.read_csv(mock_article_contents_file)

    pd.testing.assert_frame_equal(actual_article_contents_second_time, expected_article_contents_second_time)


def test_record_opinion_articles_metadata(monkeypatch):
    """
    Downloader iterates through pages and saves them to disk.
    """

    def mock_api_call(url: str, params: dict) -> Dict[str, Any]:
        """Mock functionality of making Guardian API call."""
        return {
            'response': {
                'status': 'ok',
                'userTier': 'developer',
                'total': 100,
                'startIndex': 1,
                'pageSize': 2,
                'currentPage': 1,
                'pages': 1,
                'orderBy': 'newest',
                'tag': {
                    'id': 'commentisfree/commentisfree',
                    'type': 'blog',
                    'sectionId': 'commentisfree',
                    'sectionName': 'Opinion', 'webTitle': 'Opinion',
                    'webUrl': 'https://www.theguardian.com/commentisfree/commentisfree',
                    'apiUrl': 'https://content.guardianapis.com/commentisfree/commentisfree'
                },
                'results': [
                    {
                        'id': 'commentisfree/2020/oct/04/johnson-is-a-poor-prime-minister',
                        'type': 'article',
                        'sectionId': 'commentisfree',
                        'sectionName': 'Opinion',
                        'webPublicationDate': '2020-10-04T10:35:19Z',
                        'webTitle': 'Are Tory MPs really so surprised that Boris Johnson is a poor prime minister?',
                        'webUrl': 'https://www.theguardian.com/commentisfree/2020/oct/04/poor-prime-minister',
                        'apiUrl': 'https://content.guardianapis.com/commentisfree/2020/oct/04/poor-prime-minister',
                        'isHosted': False,
                        'pillarId': 'pillar/opinion',
                        'pillarName': 'Opinion'
                    },
                    {
                        'id': 'commentisfree/2020/oct/04/university-in-a-pandemic',
                        'type': 'article',
                        'sectionId': 'commentisfree',
                        'sectionName': 'Opinion',
                        'webPublicationDate': '2020-10-04T07:30:45Z',
                        'webTitle': 'Up close the trials of university life in a pandemic. We should have done better.',
                        'webUrl': 'https://www.theguardian.com/commentisfree/2020/oct/04/university-in-a-pandemic',
                        'apiUrl': 'https://content.guardianapis.com/commentisfree/2020/oct/04/university-in-a-pandemic',
                        'isHosted': False,
                        'pillarId': 'pillar/opinion',
                        'pillarName': 'Opinion'
                    }
                ]
            }
        }

    # Set up downloader but overwrite API call with mock data
    article_downloader = the_guardian.ArticleDownloader()
    monkeypatch.setattr(article_downloader, "_call_api_and_display_exceptions", mock_api_call)

    article_downloader.record_opinion_articles_metadata()

    expected_metadata = pd.DataFrame(
        {
            'id': [
                'e8c5e312fae36c43d965a0e3da84e68d',
                '052015a6d57893adfa4be70521b1ad3b',
                '7d2669e5a86f5a5eb16862f691482fe3',
                '069738f52edca2125142e0952dbbfcc0'
            ],
            'guardian_id': [
                'politics/1990/nov/23/past.conservatives',
                'world/2002/feb/25/race.uk',
                'commentisfree/2020/oct/04/johnson-is-a-poor-prime-minister',
                'commentisfree/2020/oct/04/university-in-a-pandemic'
            ],
            'content_type': ['article', 'article', 'article', 'article'],
            'section_id': ['commentisfree', 'commentisfree', 'commentisfree', 'commentisfree'],
            'section_name': ['Opinion', 'Opinion', 'Opinion', 'Opinion'],
            'web_publication_timestamp': [
                datetime.datetime(1990, 11, 23, 16, 47, 0, 0),
                datetime.datetime(2002, 2, 25, 1, 53, 0, 0),
                datetime.datetime(2020, 10, 4, 10, 35, 19, 0),
                datetime.datetime(2020, 10, 4, 7, 30, 45, 0),
            ],
            'web_title': [
                'The Thatcher Years | Hugo Young',
                'Gary Younge: Terms of abuse',
                'Are Tory MPs really so surprised that Boris Johnson is a poor prime minister?',
                'Up close the trials of university life in a pandemic. We should have done better.'
            ],
            'web_url': [
                'https://www.theguardian.com/politics/1990/nov/23/past.conservatives',
                'https://www.theguardian.com/world/2002/feb/25/race.uk',
                'https://www.theguardian.com/commentisfree/2020/oct/04/poor-prime-minister',
                'https://www.theguardian.com/commentisfree/2020/oct/04/university-in-a-pandemic'
            ],
            'api_url': [
                'https://content.guardianapis.com/politics/1990/nov/23/past.conservatives',
                'https://content.guardianapis.com/world/2002/feb/25/race.uk',
                'https://content.guardianapis.com/commentisfree/2020/oct/04/poor-prime-minister',
                'https://content.guardianapis.com/commentisfree/2020/oct/04/university-in-a-pandemic'
            ],
            'pillar_id': ['pillar/opinion', 'pillar/opinion', 'pillar/opinion', 'pillar/opinion'],
            'pillar_name': ['Opinion', 'Opinion', 'Opinion', 'Opinion']
        }
    )

    db_connection = postgresql.DatabaseConnection()

    actual_metadata = db_connection.get_dataframe(table_name='article_metadata', schema='the_guardian')

    # Tidy up and delete newly inserted rows
    db_connection._create_connection()
    with db_connection._conn.cursor() as curs:
        curs.execute(
            """
            DELETE FROM the_guardian.article_metadata
            WHERE id IN ('7d2669e5a86f5a5eb16862f691482fe3', '069738f52edca2125142e0952dbbfcc0');
            """
        )

        db_connection._conn.commit()
    db_connection._close_connection()

    pd.testing.assert_frame_equal(actual_metadata, expected_metadata)
