"""Testing of common methods which can be re-used in different contexts of the service."""

# Third party libraries
import pytest
import requests

# Internal imports
from interlocutor.commons import commons


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
    def test_retry_correct_number_of_attempts(self, attempts_requested, successful_call_achieved, total_attempts_made, exceptions_raised):
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
