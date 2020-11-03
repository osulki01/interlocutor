"""Interact with postgres database running on database container."""

# Standard libraries
import os
import pathlib

# Third party libraries
import dotenv
import psycopg2

# Internal imports
from interlocutor import commons


class DatabaseConnection:
    """Helper class to connect and execute code against postgres database."""

    def __init__(self, database: str = 'interlocutor'):
        """
        Parameters
        ----------
        database : str (default 'interlocutor')
            Name of the database to connect to.
        """

        self._database = database

        # Load database credentials from .env file
        parent_directory = os.path.dirname(os.path.abspath(__file__))
        project_root = pathlib.Path(parent_directory).parent.parent
        dotenv.load_dotenv(dotenv_path=f"{str(project_root)}/.env")
        self._username = os.getenv('POSTGRES_USER')
        self._password = os.getenv('POSTGRES_PASSWORD')

        # Retrieve the name of the container running the database
        docker_compose_config = commons.load_docker_compose_config(f"{project_root}/docker-compose.yaml")
        self._db_container_name = docker_compose_config['services']['db']['container_name']

    def _create_connection(self):
        """Establish connection to postgres database running on container."""

        self._conn = psycopg2.connect(
            dbname=self._database,
            user=self._username,
            password=self._password,
            host=self._db_container_name
        )
