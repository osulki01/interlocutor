"""Interact with postgres database running on database container."""

# Standard libraries
import os
import pathlib
from typing import Dict

# Third party libraries
import dotenv
import pandas as pd
import psycopg2
from psycopg2 import sql as psy_sql

# Internal imports
from interlocutor.commons import commons


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
        # parent_directory = os.path.dirname(os.path.abspath(__file__))
        # project_root = pathlib.Path(parent_directory).parent.parent
        # dotenv.load_dotenv(dotenv_path=f"{str(project_root)}/.env")
        self._username = os.getenv('POSTGRES_USER')
        self._password = os.getenv('POSTGRES_PASSWORD')

        # Retrieve the name of the container running the database
        # docker_compose_config = commons.load_docker_compose_config(f"{project_root}/docker-compose.yaml")
        # self._db_container_name = docker_compose_config['services']['db_staging']['container_name']
        self._db_container_name = 'db_staging'

    def _create_connection(self):
        """Establish connection to postgres database running on container."""

        self._conn = psycopg2.connect(
            dbname=self._database,
            user=self._username,
            password=self._password,
            host=self._db_container_name
        )

    def _close_connection(self):
        """Close connection to postgres database."""

        self._conn.close()

    def get_dataframe(
            self,
            table_name: str = None,
            schema: str = None,
            query: str = None,
            query_params: Dict = None
    ) -> pd.DataFrame:
        """
        Execute query against database and retrieve the result as a pandas DataFrame.

        Parameters
        ----------
        table_name : str (default None)
            Name of table to load. Only use if you want to pull all data from a table rather than execute a specific
            query.
        schema : str (default None)
            Name of schema in which the table sits. Only use if you want to pull all data from a table rather than
            execute a specific query
        query : str (default None)
            SQL query to be executed.
        query_params : dict (default None)
            Parameters to pass to the SQL execution. Used named placeholders in the query and then provide the argument
            mapping in a dictionary e.g.

            query="SELECT * FROM schema.table WHERE id = %(specific_id)s;",
            query_params={'specific_id': 9}

            See more info here: https://www.psycopg.org/docs/usage.html#query-parameters

        Returns
        -------
        pandas DataFrame
            Either the result of the query or all of the data from `table_name`.

        Raises
        ------
        ValueError
            If both `table_name` and `query` are provided.
            If a `table_name` is set but no `schema`.
            If `query_params` have been set but no `query`.
        """

        if table_name and query:
            raise ValueError("Only one of `table_name` or `query` can be used.")

        if table_name and not schema:
            raise ValueError("Both a `schema` and `table_name` must be provided.")

        if query_params and not query:
            raise ValueError("`query_params` have been provided but no `query` to use them in")

        self._create_connection()

        if table_name:
            query_with_table = psy_sql.SQL("SELECT * FROM {}").format(psy_sql.Identifier(schema, table_name))
            df = pd.read_sql(sql=query_with_table, con=self._conn)
        else:
            df = pd.read_sql(sql=query, con=self._conn, params=query_params)

        self._close_connection()

        return df
