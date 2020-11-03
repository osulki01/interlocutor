"""Common methods which can be re-used in different contexts of the service."""

# Standard libraries
import functools
import subprocess

from typing import Callable, Tuple, Union, Dict, List

import yaml


def load_docker_compose_config(yaml_filename_path: str = 'docker-compose.yaml') -> Dict:
    """
    Loads the contents of 'docker-compose.yaml' to a dictionary so its contents can be used programmatically.

    Parameters
    ----------
    yaml_filename_path : str
        Filename or path of the docker-compose.yaml file.

    Returns
    -------
    dict
        Dictionary of the contents of 'docker-compose.yaml' file.
    """

    with open(yaml_filename_path, 'rt') as yml_file:
        return yaml.load(yml_file, Loader=yaml.FullLoader)


def retry(total_attempts: int, exceptions_to_check: Union[Exception, Tuple[Exception]]) -> Callable:
    """
    Execute the decorated function and retry a specified number of times if it encounters an exception (assumes that
    the function which makes the request explicitly raises an exception).

    Parameters
    ----------
    total_attempts : int
        Number of times that the decorated function should attempt to be executed. Should be >= 2, otherwise the
        decorated function will not retry.

    exceptions_to_check : Exception, or Tuple of Exceptions
        The types of Exception that mean the decorated function should retry.

    Returns
    -------
    Callable
        Decorator function which alters the behaviour of a function to retry the requested number of times if it
        encounters a specific Exception.

    Raises
    ------
    Exception
        If maximum number of request attempts reached without success. The Exception is the type of Exception raised
        by the decorated function on the final attempt.
    """

    def retry_decorator(func):

        @functools.wraps(func)
        def func_with_retries(*args, **kwargs):

            attempt_number = 1

            while attempt_number <= total_attempts:
                try:
                    return func(*args, **kwargs)

                except exceptions_to_check as raised_exception:

                    print(f'Function {func.__name__} failed on attempt {attempt_number} of {total_attempts} total '
                          f'attempts.')

                    if attempt_number == total_attempts:
                        print('Max attempts reached. Stopping now.')
                        raise raised_exception

                    attempt_number += 1
                    print('Retrying now.')

        return func_with_retries

    return retry_decorator


def run_cli_command_and_display_exception(cli_command: List[str]) -> None:
    """
    Execute a CLI command and display exception if one gets raised.

    Parameters
    ----------
    cli_command : list[str]
        List of individual components of the CLI command e.g. ["docker-compose", "up", "-d"]

    Raises
    ------
    subprocess.CalledProcessError
        If problem encountered running CLI command.
    """

    try:
        subprocess.run(cli_command, stderr=subprocess.PIPE, text=True, check=True)
    except subprocess.CalledProcessError as called_process_exception:
        print(called_process_exception.stderr)
        raise called_process_exception
