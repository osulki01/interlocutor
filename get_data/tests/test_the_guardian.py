# Third party libraries
import pytest
import requests

# Internal imports
from data import the_guardian


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
