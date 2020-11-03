# Standard libraries
import argparse
import os
import shutil

# Third party libraries
import bullet
import dotenv

# Internal imports
from interlocutor.commons import commons


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

    commons.run_cli_command_and_display_exception(build_services_command)


def start_database_service(clean_and_rebuild: bool, service_name: str = "db") -> None:
    """
    Start the database service, initialised the database and user if they don't already exist.

    Parameters
    ----------
    clean_and_rebuild : bool
        Whether to wipe the directory ./data/postgres and initialise the database from scratch.
    service_name : str (default "dev")
        Name of the database service in docker-compose.yaml.

    Raises
    ------
    subprocess.CalledProcessError
        If "docker-compose run" command cannot run.
    """

    docker_compose_config = commons.load_docker_compose_config()
    container_name = docker_compose_config['services'][service_name]['container_name']
    db_volume = docker_compose_config['services'][service_name]['volumes'][0].split(":")[0]

    if clean_and_rebuild and os.path.isdir(db_volume):

        prompt = bullet.Bullet(
            prompt=f"\nYou have set the flag -c, or --clean_and_rebuild_db, which will delete the directory "
                   f"{db_volume}. Confirm whether you want to delete existing data? ",
            choices=["Yes, delete and start from fresh", "No, keep existing data"],
            bullet="→",
            margin=2,
            bullet_color=bullet.colors.bright(bullet.colors.foreground["cyan"]),
            background_color=bullet.colors.background["black"],
            background_on_switch=bullet.colors.background["black"],
            word_color=bullet.colors.foreground["white"],
            word_on_switch=bullet.colors.foreground["red"]
        )

        decision = prompt.launch()

        if decision == "Yes, delete and start from fresh":
            print(f"Deleting and recreating empty version of directory {db_volume}")
            shutil.rmtree(db_volume)

        else:
            print("Database service will still be created but the data volume will not be wiped.")

    # Set up the parameters required to run the database service
    dotenv.load_dotenv(dotenv_path='.env')

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

    commons.run_cli_command_and_display_exception(run_db_service_command)

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

    docker_compose_config = commons.load_docker_compose_config()
    container_name = docker_compose_config['services'][service_name]['container_name']

    print(f"Initialising development service {service_name}")

    run_dev_service_command = ["docker-compose", "up", "--detach", f"{container_name}"]

    commons.run_cli_command_and_display_exception(run_dev_service_command)

    print(f"\nDev container running. Use 'docker exec -it {container_name} bash' to attach to container.")


def stop_and_remove_existing_containers() -> None:
    """
    Stop containers and removes containers, networks, volumes, and images created by docker-compose up.
    """

    print("Stop containers and removes containers, networks, volumes, and images created by docker-compose up.")

    stop_and_remove_command = ["docker-compose", "down"]

    commons.run_cli_command_and_display_exception(stop_and_remove_command)


if __name__ == '__main__':

    stop_and_remove_existing_containers()

    build_docker_services()

    parser = argparse.ArgumentParser(description='Start docker services.')

    parser.add_argument(
        "-c", "--clean_and_rebuild_db",
        help="Whether to wipe the directory ./data/postgres and initialise the database from scratch.",
        action="store_true",
    )

    args = parser.parse_args()

    start_database_service(clean_and_rebuild=args.clean_and_rebuild_db)

    # conn = psycopg2.connect(dbname="articles", user="dev_user", password="postgres_password", host="db_container")

    start_dev_container()
