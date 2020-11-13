"""Interact with postgres database running on database container."""

# Standard libraries
import os
import pathlib
from typing import Dict

# Third party libraries
import pandas as pd
import psycopg2
from psycopg2 import sql as psy_sql
import sqlalchemy

# Internal imports
from interlocutor.commons import commons


class DatabaseConnection:
    """Helper class to connect and execute code against postgres database."""

    def __init__(self, environment: str, database: str = 'interlocutor'):
        """
        Parameters
        ----------
        environment : str
            Deployment environment, either 'stg' or 'prd'.
        database : str (default 'interlocutor')
            Name of the database to connect to.
        """

        self._database = database
        self._username = os.getenv('POSTGRES_USER')
        self._password = os.getenv('POSTGRES_PASSWORD')

        # Retrieve the name of the database container within the deployment environment
        parent_directory = os.path.dirname(os.path.abspath(__file__))
        project_root = pathlib.Path(parent_directory).parent.parent
        docker_compose_config = commons.load_docker_compose_config(f"{project_root}/docker-compose.yml")

        self._db_container_name = docker_compose_config['services'][f'db_{environment}']['container_name']
        self._postgres_port = 5432

    def _create_connection(self) -> None:
        """Establish connection to postgres database running on container."""

        self._conn = psycopg2.connect(
            dbname=self._database,
            user=self._username,
            password=self._password,
            host=self._db_container_name
        )

        engine_connection_string = f"postgresql+psycopg2://{self._username}:{self._password}@" \
                                   f"{self._db_container_name}:{self._postgres_port}/{self._database}"

        self._engine = sqlalchemy.create_engine(engine_connection_string)

    def _close_connection(self) -> None:
        """Close connection to postgres database."""

        self._conn.close()
        self._engine.dispose()

    def execute_database_operation(self, sql_command: str, params: Dict = None) -> None:
        """
        Executes operation on database.

        Parameters
        ----------
        sql_command : str
            Database operation to be executed.
        params : dict (default None)
            Parameters to pass to the SQL execution. Used named placeholders in the query and then provide the argument
            mapping in a dictionary e.g.

            query="DELETE FROM schema.table WHERE id = %(specific_id)s;",
            query_params={'specific_id': 9}

            See more info here: https://www.psycopg.org/docs/usage.html#query-parameters
        """

        self._create_connection()

        with self._conn.cursor() as curs:
            print(curs.mogrify(sql_command, params))
            curs.execute(query=sql_command, vars=params)
            self._conn.commit()

        self._close_connection()

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

    def upload_dataframe(
            self,
            dataframe: pd.DataFrame,
            table_name: str,
            schema: str,
            **pandas_to_sql_kwargs,
    ) -> None:
        """
        Write contents of a pandas DataFrame to a permanent table on postgres database.

        Parameters
        ----------
        dataframe : pandas DataFrame
            Data to be uploaded.
        table_name : str
            Name of target table which will store the dataframe.
        schema : str (default None)
            Name of schema in which the target table sits.
        pandas_to_sql_kwargs : additional named arguments
            Arguments which can be passed to the pandas.DataFrame.to_sql command.
        """

        self._create_connection()

        dataframe.to_sql(con=self._engine, name=table_name, schema=schema, **pandas_to_sql_kwargs)

        self._close_connection()
