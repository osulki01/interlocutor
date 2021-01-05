"""
Testing the interaction with postgres database running on database container.

The tests make heavy use of the testing table stored in interlocutor.testing_schema.testing_table. To see what the data
looks like, you can query that table or see the file Docker/db/staging_data/testing_schema.testing_table.csv from the
repository root.
"""

# Standard libraries
import datetime
import time

# Third party libraries
import pandas as pd
import psycopg2
import pytest

# Internal imports
from interlocutor.database import postgresql


def test_check_database_is_live(monkeypatch):
    """Exception is raised if database is not alive and accepting connections after pausing and retrying once more."""

    # SCENARIO 1: Connection succeeds on first attempt
    db_connection_succeed_first_time = postgresql.DatabaseConnection()
    assert db_connection_succeed_first_time._initial_database_response_success

    # SCENARIO 2: Connection does not succeed at all

    # Avoid having to wait before retrying
    def mock_sleep(secs: int = None):
        """Mock how the sleep function is called but do not actually delay code running."""
        pass

    monkeypatch.setattr(time, 'sleep', mock_sleep)

    with pytest.raises(psycopg2.OperationalError):
        db_connection_incorrect_database = postgresql.DatabaseConnection(database='incorrect_database')


def test_execute_database_operation():
    """SQL command successfully executed on database."""

    db_connection = postgresql.DatabaseConnection()

    # Mock data which will be inserted into permanent testing table as a new row
    example_integer = 3
    example_string = 'Third value'
    example_timestamp = datetime.datetime(2020, 11, 10, 15, 20, 37, 0)

    db_connection.execute_database_operation(
        sql_command=("INSERT INTO testing_schema.testing_table(example_integer, example_string, example_timestamp) "
                     "VALUES (%(example_integer)s, %(example_string)s, %(example_timestamp)s)"),
        params={
            'example_integer': example_integer,
            'example_string': example_string,
            'example_timestamp': example_timestamp
        }
    )

    # Outline what the new row should look like
    expected_df = pd.DataFrame(
        data=[[example_integer, example_string, example_timestamp]],
        columns=['example_integer', 'example_string', 'example_timestamp']
    )

    # Query the table to see if the new row was inserted correctly
    db_connection._create_connection()
    with db_connection._conn.cursor() as cursor:
        cursor.execute(
            query="SELECT * FROM testing_schema.testing_table WHERE example_integer = %(example_integer)s;",
            vars={'example_integer': example_integer}
        )

        table_tuples = cursor.fetchall()
        actual_df = pd.DataFrame(data=table_tuples, columns=['example_integer', 'example_string', 'example_timestamp'])

        # Tidy up
        db_connection.execute_database_operation(
            sql_command="DELETE FROM testing_schema.testing_table WHERE example_integer = %(example_integer)s",
            params={'example_integer': example_integer}
        )

    db_connection._close_connection()

    pd.testing.assert_frame_equal(left=actual_df, right=expected_df)


def test_get_column_names_existing_table():
    """The column names of an existing table are successfully retrieved."""

    db_connection = postgresql.DatabaseConnection()

    expected_column_names = ['example_integer', 'example_string', 'example_timestamp']

    actual_column_names = db_connection._get_column_names_existing_table(
        table_name='testing_table',
        schema='testing_schema'
    )

    assert all([actual == expected for actual, expected in zip(actual_column_names, expected_column_names)])

    # Appropriate exception is raised if column names cannot be retrieved
    with pytest.raises(expected_exception=pd.io.sql.DatabaseError, match='Execution failed on sql*'):

        db_connection._get_column_names_existing_table(table_name='non_existent_table', schema='testing_schema')


def test_get_dataframe_demands_correct_arguments():
    """Exceptions or warnings should be raised if an incorrect combination of arguments is provided."""

    db_connection = postgresql.DatabaseConnection()

    # Scenario 1: Both query and table name are provided. Only one should be provided to either execute a query or
    # retrieve a full table.
    mock_query = "SELECT * FROM schema.table;"
    mock_table_name = "table"

    with pytest.raises(expected_exception=ValueError, match="Only one of `table_name` or `query` can be used."):
        db_connection.get_dataframe(query=mock_query, table_name=mock_table_name)

    # Scenario 2: Table name is provided but not the schema in which it resides.
    with pytest.raises(expected_exception=ValueError, match="Both a `schema` and `table_name` must be provided."):
        db_connection.get_dataframe(table_name=mock_table_name)

    # Scenario 3: Query parameters are provided but no query to use them in.
    with pytest.raises(
            expected_exception=ValueError,
            match="`query_params` have been provided but no `query` to use them in"
    ):
        db_connection.get_dataframe(query_params={'mock_arg': 'mock_value'})


def test_get_dataframe_from_table_name():
    """All data from a table can be retrieved simply by providing a schema and table name."""

    db_connection = postgresql.DatabaseConnection()

    expected = pd.DataFrame(
        columns=['example_integer', 'example_string', 'example_timestamp'],
        data=[[1, 'First value', '2020-01-21T01:53:00Z'], [2, 'Second value', '2020-07-16T03:31:00Z']]
    )

    expected['example_timestamp'] = pd.to_datetime(expected['example_timestamp'], format='%Y-%m-%dT%H:%M:%SZ')

    actual = db_connection.get_dataframe(schema="testing_schema", table_name="testing_table")

    pd.testing.assert_frame_equal(left=actual, right=expected)


def test_get_dataframe_from_query():
    """All data from a table can be retrieved by providing a query."""

    db_connection = postgresql.DatabaseConnection()

    expected = pd.DataFrame(columns=['example_integer', 'example_string'], data=[[1, 'First value']])

    actual = db_connection.get_dataframe(
        query="SELECT example_integer, example_string FROM testing_schema.testing_table WHERE example_integer = 1;"
    )

    pd.testing.assert_frame_equal(left=actual, right=expected)


def test_get_dataframe_from_query_with_parameters():
    """All data from a table can be retrieved by providing a parameterised query."""

    db_connection = postgresql.DatabaseConnection()

    expected = pd.DataFrame(columns=['example_integer', 'example_string'], data=[[1, 'First value']])

    actual = db_connection.get_dataframe(
        query=("SELECT example_integer, example_string "
               "FROM testing_schema.testing_table WHERE example_integer = %(id)s;"),
        query_params={'id': 1}
    )

    pd.testing.assert_frame_equal(left=actual, right=expected)


def test_get_min_or_max_from_column():
    """The minimum or maximum value from a column is returned."""

    db_connection = postgresql.DatabaseConnection()

    common_args = {'table_name': 'testing_table', 'schema': 'testing_schema', 'column': 'example_integer'}

    # Expects correct inputs
    with pytest.raises(ValueError, match="The `min_or_max` argument must be either 'min' or 'max'."):
        db_connection.get_min_or_max_from_column(**common_args, min_or_max='standard_dev')

    # Retrieves the minimum
    expected_min = 1

    actual_min = db_connection.get_min_or_max_from_column(**common_args, min_or_max='min')

    assert actual_min == expected_min

    # Retrieves the maximum
    expected_max = 2
    actual_max = db_connection.get_min_or_max_from_column(**common_args, min_or_max='max')

    assert actual_max == expected_max


def test_upload_dataframe():
    """Dataframe is successfully written to a permanent postgres table."""

    db_connection = postgresql.DatabaseConnection()

    # Create dataframe and upload to postgres
    expected_df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})
    db_connection.upload_dataframe(
        dataframe=expected_df,
        schema='testing_schema',
        table_name='uploaded_dataframe',
        index=False,
    )

    # Retrieve the table and see if it was uploaded correctly
    db_connection._create_connection()
    with db_connection._conn.cursor() as cursor:
        cursor.execute('SELECT * FROM testing_schema.uploaded_dataframe;')

        table_tuples = cursor.fetchall()
        actual_df = pd.DataFrame(table_tuples, columns=['col1', 'col2'])

        # Tidy up
        cursor.execute('DROP TABLE testing_schema.uploaded_dataframe;')
        db_connection._conn.commit()

    pd.testing.assert_frame_equal(left=actual_df, right=expected_df)


def test_upload_new_data_only_to_existing_table_inserts_new_rows_only():
    """Only new rows are inserted into an existing table."""

    db_connection = postgresql.DatabaseConnection()

    # Create dataframe with one row where the example_integer index already exists in the target table and should not be
    # inserted (1), and a new index (99) which should go in.
    # Also reorganise the rows to a different order to make sure it is handled
    rows_to_upload = pd.DataFrame(
        data={
            'example_string': ["Won't be inserted", "Will be inserted"],
            'example_integer': [1, 99],
            'example_timestamp': [datetime.datetime(2020, 11, 10, 15, 20, 37, 0),
                                  datetime.datetime(2035, 6, 10, 19, 3, 4, 0)]
        }
    )

    db_connection.upload_new_data_only_to_existing_table(
        dataframe=rows_to_upload,
        table_name='testing_table',
        schema='testing_schema',
        id_column='example_integer'
    )

    # Retrieve the table and see if it was inserted into correctly
    expected_df = pd.DataFrame(
        data={
            'example_integer': [1, 2, 99],
            'example_string': ["First value", "Second value", "Will be inserted"],
            'example_timestamp': [datetime.datetime(2020, 1, 21, 1, 53, 0, 0),
                                  datetime.datetime(2020, 7, 16, 3, 31, 0, 0),
                                  datetime.datetime(2035, 6, 10, 19, 3, 4, 0)]
        }
    )

    db_connection._create_connection()
    with db_connection._conn.cursor() as cursor:
        cursor.execute('SELECT * FROM testing_schema.testing_table;')

        table_tuples = cursor.fetchall()
        actual_df = pd.DataFrame(table_tuples, columns=['example_integer', 'example_string', 'example_timestamp'])

        # Tidy up
        cursor.execute('DELETE FROM testing_schema.testing_table WHERE example_integer = 99;')
        db_connection._conn.commit()

    pd.testing.assert_frame_equal(left=actual_df, right=expected_df)


def test_upload_new_data_only_to_existing_table_raises_exception_with_different_columns():
    """Exception is raised with helpful message if columns in dataframe are not identical to the target table."""

    db_connection = postgresql.DatabaseConnection()

    # Scenario 1, column exists in local dataframe but not target table
    scenario_1_rows_to_upload = pd.DataFrame(
        data={
            'example_string': ["New row"],
            'example_integer': [6],
            'example_timestamp': [datetime.datetime(2020, 11, 10, 15, 20, 37, 0)],
            'non_existent_column_in_target_table': [99]
        }
    )

    with pytest.raises(
            ValueError,
            match="The column names in the dataframe are not identical to that of the target table."
    ):
        db_connection.upload_new_data_only_to_existing_table(
            dataframe=scenario_1_rows_to_upload,
            table_name='testing_table',
            schema='testing_schema',
            id_column='example_integer'
        )

    # Scenario 2, column exists in target table but not local dataframe
    # example_timestamp column no longer included
    scenario_2_rows_to_upload = pd.DataFrame(
        data={
            'example_string': ["New row"],
            'example_integer': [6],
            # 'example_timestamp': [datetime.datetime(2020, 11, 10, 15, 20, 37, 0)],
        }
    )

    with pytest.raises(
            ValueError,
            match="The column names in the dataframe are not identical to that of the target table."
    ):
        db_connection.upload_new_data_only_to_existing_table(
            dataframe=scenario_2_rows_to_upload,
            table_name='testing_table',
            schema='testing_schema',
            id_column='example_integer'
        )
