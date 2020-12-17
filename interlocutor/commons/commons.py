"""Common methods which can be re-used in different contexts of the service."""

# Standard libraries
import functools
import os
import subprocess
import time
from typing import Callable, Dict, List, Tuple, Union

# Third party libraries
import pandas as pd
import yaml


def load_docker_compose_config(yaml_filename_path: str = 'docker-compose.yml') -> Dict:
    """
    Loads the contents of 'docker-compose.yml' to a dictionary so its contents can be used programmatically.

    Parameters
    ----------
    yaml_filename_path : str
        Filename or path of the docker-compose.yml file.

    Returns
    -------
    dict
        Dictionary of the contents of 'docker-compose.yml' file.
    """

    with open(yaml_filename_path, 'rt') as yml_file:
        return yaml.load(yml_file, Loader=yaml.FullLoader)


def retry(
        total_attempts: int,
        exceptions_to_check: Union[Exception, Tuple[Exception]],
        seconds_to_wait: int = None
) -> Callable:
    """
    Execute the decorated function and retry a specified number of times if it encounters an exception.

    Parameters
    ----------
    total_attempts : int
        Number of times that the decorated function should attempt to be executed. Should be >= 2, otherwise the
        decorated function will not retry.

    exceptions_to_check : Exception, or Tuple of Exceptions
        The types of Exception that mean the decorated function should retry.

    seconds_to_wait : int (default None)
        How many seconds to wait until trying again.

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

                    if seconds_to_wait:
                        print(f'Waiting {seconds_to_wait} seconds before trying again')
                        time.sleep(seconds_to_wait)

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


def write_or_append_dataframe_to_csv(data: pd.DataFrame, filepath: str) -> None:
    """
    Save data to a csv file (and append if it already exists).

    Parameters
    ----------
    data : pandas.DataFrame
        Data to be written to disk.
    filepath : str
        File name/path which in which to store the data.
    """

    if os.path.isfile(filepath):
        write_mode = 'a'
        header = False
    else:
        write_mode = 'w'
        header = True

    data.to_csv(path_or_buf=filepath, header=header, index=False, mode=write_mode)
