"""Interact with postgres database running on database container."""

# Standard libraries
import os
import pathlib
from typing import Any, Dict, List, Union

# Third party libraries
import pandas as pd
import psycopg2
from psycopg2 import sql as psy_sql
import sqlalchemy

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

        # Ensure dev environment only connects to staging as no 'dev' database exists
        environment = 'stg' if os.getenv('DEPLOYMENT_ENVIRONMENT') == 'dev' else os.getenv('DEPLOYMENT_ENVIRONMENT')

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

    def execute_database_operation(self, sql_command: Union[str, psy_sql.Composable], params: Dict = None) -> None:
        """
        Executes operation on database.

        Parameters
        ----------
        sql_command : str or psycopg2.sql.Composable
            Database operation to be executed.
        params : dict (default None)
            Parameters to pass to the SQL execution. Used named placeholders in the query and then provide the argument
            mapping in a dictionary e.g.

            sql_command="DELETE FROM schema.table WHERE id = %(specific_id)s;",
            params={'specific_id': 9}

            See more info here: https://www.psycopg.org/docs/usage.html#query-parameters
        """

        self._create_connection()

        with self._conn.cursor() as curs:
            curs.execute(query=sql_command, vars=params)
            self._conn.commit()

        self._close_connection()

    def _get_column_names_existing_table(
            self,
            table_name: str,
            schema: str
    ) -> List[str]:
        """
        Get the list of column names from an existing table.

        Parameters
        ----------
        table_name : str
            Name of target table which will store the dataframe.
        schema : str (default None)
            Name of schema in which the target table sits.

        Returns
        -------
        list
            All of the column names in the same order that they exist in postgres.
        """

        sql_query = psy_sql.SQL("SELECT * FROM {} LIMIT 0;").format(psy_sql.Identifier(schema, table_name))

        try:
            existing_df = self.get_dataframe(query=sql_query)
        except pd.io.sql.DatabaseError as db_error:
            print(
                f"Unable to get columns from table {schema}.{table_name}. Are you sure it exists and you have access?"
            )
            raise db_error

        return existing_df.columns.values

    def get_dataframe(
            self,
            table_name: str = None,
            schema: str = None,
            query: Union[str, psy_sql.Composable] = None,
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
        query : str or psycopg2.sql.Composable (default None)
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

    def get_min_or_max_from_column(self, table_name: str, schema: str, min_or_max: str, column: str) -> Any:
        """
        Get the minimum or maximum value from a column in a postgres table.

        Parameters
        ----------
        table_name : str
            Name of postgres table to be queried.
        schema : str (default None)
            Name of schema in which the table sits.
        min_or_max : str ('min' or 'max')
            Whether to retrieve the minimum or the maximum.
        column : str
            Which column to take the min or max from.

        Returns
        -------
        Any
            The minimum or maximum value from the column, which will be whatever data type corresponds to the column
            data type on postgres.

        Raises
        ------
        ValueError
            If `min_or_max` is neither 'min' nor 'max'.
        """

        if min_or_max == 'min':
            sql_query = psy_sql.SQL("SELECT MIN({column}) FROM {schema_and_table}").format(
                column=psy_sql.Identifier(column),
                schema_and_table=psy_sql.Identifier(schema, table_name)
            )

        elif min_or_max == 'max':
            sql_query = psy_sql.SQL("SELECT MAX({column}) FROM {schema_and_table}").format(
                column=psy_sql.Identifier(column),
                schema_and_table=psy_sql.Identifier(schema, table_name)
            )

        else:
            raise ValueError("The `min_or_max` argument must be either 'min' or 'max'.")

        min_or_max_column = self.get_dataframe(query=sql_query)

        return min_or_max_column[min_or_max].values[0]

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

    def upload_new_data_only_to_existing_table(
            self,
            dataframe: pd.DataFrame,
            table_name: str,
            schema: str,
            id_column: str
    ) -> None:
        """
        Write contents of a pandas DataFrame to an existing table on postgres database, but only insert new rows.

        This is not an insert statement, not an upsert, so rows which share the same ID as an existing entry are simply
        ignored rather than updating existing entries in the target table.

        Parameters
        ----------
        dataframe : pandas DataFrame
            Data to be uploaded.
        table_name : str
            Name of target table which will store the dataframe.
        schema : str (default None)
            Name of schema in which the target table sits.
        id_column : str
            Primary key column in target table which identifies whether a row already exists.

        Raises
        ------
        ValueError
            If columns in the `dataframe` are not identical to the target `table_name`.
        """

        # Get column names from target table and make sure dataframe is in the same order
        postgres_table_columns = self._get_column_names_existing_table(table_name=table_name, schema=schema)
        set_postgres_table_columns = set(postgres_table_columns)
        set_dataframe_columns = set(dataframe.columns)

        if set_dataframe_columns != set_postgres_table_columns:
            print("The columns that do not exist in both the local dataframe and target table are...")
            print(set_dataframe_columns.symmetric_difference(set_postgres_table_columns))
            raise ValueError("The column names in the dataframe are not identical to that of the target table.")

        dataframe_reorganised_columns = dataframe.reindex(columns=postgres_table_columns)

        # Create staging table which will store data intermediately
        staging_table_name = f"{table_name}_staging"
        self.upload_dataframe(
            dataframe=dataframe_reorganised_columns,
            table_name=staging_table_name,
            schema=schema,
            index=False
        )

        try:
            # Transfer new rows from staging to target table
            insert_query = psy_sql.SQL("INSERT INTO {target_schema_and_table} "
                                       "SELECT * FROM {staging_table_schema_and_table} "
                                       "WHERE {id_column} NOT IN "
                                       "(SELECT DISTINCT {id_column} FROM {target_schema_and_table})").format(
                target_schema_and_table=psy_sql.Identifier(schema, table_name),
                staging_table_schema_and_table=psy_sql.Identifier(schema, staging_table_name),
                id_column=psy_sql.Identifier(id_column)
            )

            self._create_connection()

            with self._conn.cursor() as curs:
                curs.execute(query=insert_query)
                self._conn.commit()

            self._close_connection()

        finally:
            # Drop intermediate staging table
            self.execute_database_operation(
                sql_command=psy_sql.SQL("DROP TABLE {staging_table_schema_and_table}").format(
                    staging_table_schema_and_table=psy_sql.Identifier(schema, staging_table_name)))
