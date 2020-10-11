# Standard libraries
from typing import Any, Dict

# Third party libraries
import pandas as pd
from pyfakefs.pytest_plugin import fs
import pytest
import requests

# Internal imports
from get_data import the_guardian


class RetryTracker:
    """Track how many times a function has been called when using the retry decorator."""

    def __init__(self):
        self.attempt_count = 0
        self.exceptions_raised = 0
        self.successful_call = False

    def increment_attempt_count(self):
        """Add 1 to the attempt_count every time the method is called."""
        self.attempt_count += 1

    def increment_exception_count(self):
        """Add 1 to the exception_count every time an exception is raised."""
        self.exceptions_raised += 1

    def mark_successful_call(self):
        """Flag whether the function every succeeded without raising an exception."""
        self.successful_call = True


class TestRetry:

    def succeed_on_third_request_attempt(self, retry_tracker: RetryTracker) -> None:
        """
        Raise a requests exception until the third attempt. Updates the number of attempts tried each time, and flags
        whether any of the function call succeeded without an exception.

        Parameters
        ----------
        retry_tracker : RetryTracker
            Class to track how many times the function has been called.

        Raises
        ------
        requests.exceptions.RequestException
            requests library exception on the first and second time.
        """

        retry_tracker.increment_attempt_count()

        if retry_tracker.attempt_count == 3:
            retry_tracker.mark_successful_call()
        else:
            retry_tracker.increment_exception_count()
            raise requests.exceptions.RequestException

    @pytest.mark.parametrize(
        argnames='attempts_requested,successful_call_achieved,total_attempts_made,exceptions_raised',
        argvalues=[
            # Willing to try method one time; it will never succeed and raise one exception
            (1, False, 1, 1),
            # Willing to try method two times; it will never succeed and raise exception twice
            (2, False, 2, 2),
            # Willing to try method three times; it will succeed on the third attempt and only raise two exceptions
            (3, True, 3, 2),
            # Willing to try method four times; it will succeed on the third attempt and only raise two exceptions
            (4, True, 3, 2)
        ]
    )
    def test_retry_correct_number_of_attempts(self, attempts_requested, successful_call_achieved, total_attempts_made, exceptions_raised):
        """Function decorated with the retry method retries an appropriate number of times."""

        retry_tracker = RetryTracker()

        try:
            the_guardian.retry(
                total_attempts=attempts_requested, exceptions_to_check=requests.exceptions.RequestException
            )(self.succeed_on_third_request_attempt)(retry_tracker)

        # Ignore exception if function never succeeds in order to check its history of being called
        except requests.exceptions.RequestException:
            pass

        assert retry_tracker.attempt_count == total_attempts_made
        assert retry_tracker.successful_call == successful_call_achieved
        assert retry_tracker.exceptions_raised == exceptions_raised

    def test_retry_stops_on_unexpected_exception(self):
        """
        Function decorated with the retry method will only retry if it is an exception that it was asked to handle.
        """

        retry_tracker = RetryTracker()

        non_request_exception_type = KeyError

        # Method should attempt once, raises an exception it was not asked to handle, and stops
        with pytest.raises(requests.exceptions.RequestException):
            the_guardian.retry(
                total_attempts=3, exceptions_to_check=non_request_exception_type
            )(self.succeed_on_third_request_attempt)(retry_tracker)

        assert retry_tracker.attempt_count == 1
        assert retry_tracker.successful_call is False
        assert retry_tracker.exceptions_raised == 1


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


def test_get_latest_opinion_articles_datetime_reached(fs):
    """
    Correct marker of progress made by previous API calls is retrieved, by showing the latest publication datetime
    reached.
    """

    # No previous attempts
    mock_metadata_file = 'mock_metadata_file.csv'

    article_downloader = the_guardian.ArticleDownloader(metadata_file=mock_metadata_file)

    latest_datetime_reached_no_previous_attempts = article_downloader._get_latest_opinion_articles_datetime_reached()

    assert latest_datetime_reached_no_previous_attempts is None

    # Previous attempts
    mock_metadata_dict = {
        'id': ['politics/1990/nov/23/past.conservatives', 'world/2002/feb/25/race.uk'],
        'type': ['article',	'article'],
        'sectionId': ['commentisfree',	'commentisfree'],
        'sectionName': ['Opinion',	'Opinion'],
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

    latest_datetime_reached_previous_attempts = article_downloader._get_latest_opinion_articles_datetime_reached()

    assert latest_datetime_reached_previous_attempts == '2002-02-25T01:53:00Z'


def test_record_opinion_articles(fs, monkeypatch):
    """
    Downloader iterates through pages and saves them to disk.
    """

    total_pages = 2

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
                'pages': total_pages,
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
    mock_metadata_file = 'mock_metadata_file.csv'
    article_downloader = the_guardian.ArticleDownloader(metadata_file=mock_metadata_file)
    monkeypatch.setattr(article_downloader, "_call_api_and_display_exceptions", mock_api_call)

    # Call function in order to inspect output saved to disk
    article_downloader.record_opinion_articles_metadata()

    # There are two pages in the mock data, so it is expected that the data would be pulled twice as we have forced the
    # API call to get the same response each time
    expected_metadata = pd.DataFrame(
        {
            'id': [
                'commentisfree/2020/oct/04/johnson-is-a-poor-prime-minister',
                'commentisfree/2020/oct/04/university-in-a-pandemic',
                'commentisfree/2020/oct/04/johnson-is-a-poor-prime-minister',
                'commentisfree/2020/oct/04/university-in-a-pandemic'
            ],
            'type': ['article', 'article', 'article', 'article'],
            'sectionId': ['commentisfree', 'commentisfree', 'commentisfree', 'commentisfree'],
            'sectionName': ['Opinion', 'Opinion', 'Opinion', 'Opinion'],
            'webPublicationDate': [
                '2020-10-04T10:35:19Z',
                '2020-10-04T07:30:45Z',
                '2020-10-04T10:35:19Z',
                '2020-10-04T07:30:45Z'
            ],
            'webTitle': [
                'Are Tory MPs really so surprised that Boris Johnson is a poor prime minister?',
                'Up close the trials of university life in a pandemic. We should have done better.',
                'Are Tory MPs really so surprised that Boris Johnson is a poor prime minister?',
                'Up close the trials of university life in a pandemic. We should have done better.'
            ],
            'webUrl': [
                'https://www.theguardian.com/commentisfree/2020/oct/04/poor-prime-minister',
                'https://www.theguardian.com/commentisfree/2020/oct/04/university-in-a-pandemic',
                'https://www.theguardian.com/commentisfree/2020/oct/04/poor-prime-minister',
                'https://www.theguardian.com/commentisfree/2020/oct/04/university-in-a-pandemic'
            ],
            'apiUrl': [
                'https://content.guardianapis.com/commentisfree/2020/oct/04/poor-prime-minister',
                'https://content.guardianapis.com/commentisfree/2020/oct/04/university-in-a-pandemic',
                'https://content.guardianapis.com/commentisfree/2020/oct/04/poor-prime-minister',
                'https://content.guardianapis.com/commentisfree/2020/oct/04/university-in-a-pandemic'
            ],
            'isHosted': [False, False, False, False],
            'pillarId': ['pillar/opinion', 'pillar/opinion', 'pillar/opinion', 'pillar/opinion'],
            'pillarName': ['Opinion', 'Opinion', 'Opinion', 'Opinion']
        }
    )

    actual_metadata = pd.read_csv(mock_metadata_file)

    pd.testing.assert_frame_equal(actual_metadata, expected_metadata)


def test_save_article_data_to_disk(fs):
    """Saves dataframe to file and appends if it already exists."""

    # Save data for the first time
    mock_metadata_file = 'mock_metadata_file.csv'
    article_downloader = the_guardian.ArticleDownloader(metadata_file=mock_metadata_file)

    expected_first_wave_metadata = pd.DataFrame({'column_1': ['a', 'b'], 'column_2': [True, True]})

    article_downloader._save_article_data_to_disk(expected_first_wave_metadata)

    actual_first_wave_metadata = pd.read_csv(mock_metadata_file)

    pd.testing.assert_frame_equal(actual_first_wave_metadata, expected_first_wave_metadata)

    # Append data to file
    second_wave_of_metadata = pd.DataFrame({'column_1': ['c', 'd'], 'column_2': [False, False]})

    article_downloader._save_article_data_to_disk(second_wave_of_metadata)

    actual_total_metadata = pd.read_csv(mock_metadata_file)

    expected_total_metadata = pd.concat(objs=[expected_first_wave_metadata, second_wave_of_metadata], ignore_index=True)

    pd.testing.assert_frame_equal(actual_total_metadata, expected_total_metadata)
