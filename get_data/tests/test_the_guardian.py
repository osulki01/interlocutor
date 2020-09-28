# Third party libraries
from pyfakefs.pytest_plugin import fs
import pytest
import requests

# Internal imports
from get_data import the_guardian


def test_call_api_and_handle_errors_displays_error():
    """Exception is raised and shown when API request fails."""

    url = 'http://content.guardianapis.com/tags/INCORRECT_URL'

    with pytest.raises(requests.exceptions.RequestException):
        the_guardian._call_api_and_handle_errors(url=url)


def test_call_api_and_handle_errors_makes_successful_call():
    """Calls can be successfully made against API."""

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

    actual_response = the_guardian._call_api_and_handle_errors(url=url, params=params)

    assert actual_response == expected_response


def test_get_latest_opinion_articles_page_reached(fs):
    """Correct marker of progress made by previous API calls is retrieved."""

    # No previous attempts
    progress_file = 'mock_progress_file.txt'

    expected_outcome_no_previous_attempts = 1

    actual_outcome_no_previous_attempts = the_guardian._get_latest_opinion_articles_page_reached(
        file_name_or_path=progress_file
    )

    assert actual_outcome_no_previous_attempts == expected_outcome_no_previous_attempts

    # Previous attempts

    with open(file=progress_file, mode='wt') as opened_file:
        opened_file.write('6 of 100 processed already')

    expected_outcome_previous_attempts = 6

    actual_outcome_previous_attempts = the_guardian._get_latest_opinion_articles_page_reached(
        file_name_or_path=progress_file
    )

    assert actual_outcome_previous_attempts == expected_outcome_previous_attempts
