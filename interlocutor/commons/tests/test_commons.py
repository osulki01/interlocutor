"""Testing of common methods which can be re-used in different contexts of the service."""

# Standard libraries
import os

# Third party libraries
import pandas as pd
import pytest
import requests
import subprocess

# Internal imports
from interlocutor.commons import commons


def test_load_docker_compose_config():
    """Configuration is loaded correctly from docker-compose.yml file."""

    expected_config = {
        'version': '3.8',
        'services': {
            'dev': {
                'build': './Docker/dev',
                'container_name': 'dev',
                'env_file': ['.env'],
                'ports': ['8888:8888'],
                'volumes': ['./:/usr/src/app']
            },
            'db': {
                'build': './Docker/db',
                'container_name': 'db_container',
                'ports': ['5432:5432'],
                'volumes': ['./data/postgres:/var/lib/postgresql/data']
            }
        }
    }

    current_directory = os.path.dirname(os.path.abspath(__file__))

    actual_config = commons.load_docker_compose_config(
        yaml_filename_path=f'{current_directory}/mock_docker-compose.yaml'
    )

    assert actual_config == expected_config


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


def succeed_on_third_request_attempt(retry_tracker: RetryTracker) -> None:
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


class TestRetry:

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
    def test_retry_correct_number_of_attempts(
            self,
            attempts_requested,
            successful_call_achieved,
            total_attempts_made,
            exceptions_raised
    ):
        """Function decorated with the retry method retries an appropriate number of times."""

        retry_tracker = RetryTracker()

        try:
            commons.retry(
                total_attempts=attempts_requested, exceptions_to_check=requests.exceptions.RequestException
            )(succeed_on_third_request_attempt)(retry_tracker)

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
            commons.retry(
                total_attempts=3, exceptions_to_check=non_request_exception_type
            )(succeed_on_third_request_attempt)(retry_tracker)

        assert retry_tracker.attempt_count == 1
        assert retry_tracker.successful_call is False
        assert retry_tracker.exceptions_raised == 1


# 'capsys' is a pytest fixtures which allows you to access stdout/stderr output created during test execution.
def test_run_cli_command_and_display_exception(capsys):
    """Exception is raised and shown if encountered while executing CLI command."""

    cli_command_which_will_fail = ['ls', '--invalid_flag']

    with pytest.raises(subprocess.CalledProcessError):
        commons.run_cli_command_and_display_exception(cli_command_which_will_fail)

    # The error message can be multiple lines, so check that the error message contains the right text somewhere
    # This test is an example of why you want to develop on the docker service 'dev' because the error message
    # might be different on your OS and cause this test to fail
    expected_output_message = "ls: unrecognized option '--invalid_flag'"
    actual_output_message, _ = capsys.readouterr()

    # Error message can span multiple lines or have trailing space so compare against the content only using strip
    assert expected_output_message in actual_output_message


# "fs" is the reference to the fake file system from the fixture provided by pyfakefs library
def test_save_article_data_to_disk(fs):
    """Saves dataframe to file and appends if it already exists."""

    # Save data for the first time
    mock_file = 'mock_file.csv'

    expected_first_dataframe = pd.DataFrame({'column_1': ['a', 'b'], 'column_2': [True, True]})

    commons.write_or_append_dataframe_to_csv(data=expected_first_dataframe, filepath=mock_file)

    actual_first_dataframe = pd.read_csv(mock_file)

    pd.testing.assert_frame_equal(actual_first_dataframe, expected_first_dataframe)

    # Append data to file
    second_wave_of_data = pd.DataFrame({'column_1': ['c', 'd'], 'column_2': [False, False]})

    commons.write_or_append_dataframe_to_csv(data=second_wave_of_data, filepath=mock_file)

    actual_total_dataframe = pd.read_csv(mock_file)

    expected_total_dataframe = pd.concat(objs=[expected_first_dataframe, second_wave_of_data], ignore_index=True)

    pd.testing.assert_frame_equal(actual_total_dataframe, expected_total_dataframe)
