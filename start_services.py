# Standard libraries
import os
import subprocess
from typing import Dict, List

# Third party libraries
import dotenv
import yaml


def build_docker_services() -> None:
    """
    Use docker-compose to build the services outlined in docker-compose.yml

    Raises
    ------
    subprocess.CalledProcessError
        If "docker-compose build" command cannot run.
    """

    print("Building docker services")

    build_services_command = ["docker-compose", "build"]

    _run_cli_command_and_display_exception(build_services_command)


def _load_docker_compose_config() -> Dict:
    """
    Loads the contents of 'docker-compose.yaml' to a dictionary so its contents can be used programmatically.

    Returns
    -------
    dict
        Dictionary of the contents of 'docker-compose.yaml' file.
    """

    with open('docker-compose.yaml', 'rt') as yml_file:
        return yaml.load(yml_file, Loader=yaml.FullLoader)


def _run_cli_command_and_display_exception(cli_command: List[str]) -> None:
    """
    Execute a docker CLI command and display exception if one gets raised.

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


def start_database_service(service_name: str = "db") -> None:
    """
    Start the database service, initialised the database and user if they don't already exist.

    Parameters
    ----------
    service_name : str (default "dev")
        Name of the database service in docker-compose.yaml.

    Raises
    ------
    subprocess.CalledProcessError
        If "docker-compose run" command cannot run.
    """

    # In the Apple macOS operating system, .DS_Store is a file that stores custom attributes of its containing folder.
    # The database service will ignore the creation/initialisation script if its data directory is not empty, so make
    # sure this hidden file does not exist and unintentionally make the data directory non-empty
    unwanted_ds_store_filepath = './data/postgres/.DS_Store'
    if os.path.isfile(unwanted_ds_store_filepath):
        os.remove(unwanted_ds_store_filepath)

    # Set up the parameters required to run the database service
    dotenv.load_dotenv(dotenv_path='.env')

    docker_compose_config = _load_docker_compose_config()
    container_name = docker_compose_config['services'][service_name]['container_name']

    postgres_username = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv("POSTGRES_PASSWORD")

    print(f"Initialising database service {service_name}")

    run_db_service_command = [
        "docker-compose", "run",
        "--detach",
        "-e", f"POSTGRES_USER={postgres_username}",
        "-e", f"POSTGRES_PASSWORD={postgres_password}",
        "--name", f"{container_name}",
        f"{service_name}",
    ]

    _run_cli_command_and_display_exception(run_db_service_command)

    print(f"\nDatabase service running. Use 'docker exec -it {container_name} bash' to attach to container.")


def start_dev_container(service_name: str = "dev") -> None:
    """
    Start the dev service which serves as the development environment.

    Parameters
    ----------
    service_name : str (default "dev")
        Name of the development environment service in docker-compose.yaml.

    Raises
    ------
    subprocess.CalledProcessError
        If "docker-compose up" command cannot run.
    """

    docker_compose_config = _load_docker_compose_config()
    container_name = docker_compose_config['services'][service_name]['container_name']

    print(f"Initialising development service {service_name}")

    run_dev_service_command = ["docker-compose", "up", "--detach", f"{container_name}"]

    _run_cli_command_and_display_exception(run_dev_service_command)

    print(f"\nDev container running. Use 'docker exec -it {container_name} bash' to attach to container.")


if __name__ == '__main__':

    build_docker_services()

    start_database_service()

    # conn = psycopg2.connect(dbname="articles", user="dev_user", password="postgres_password", host="db_container")

    start_dev_container()
