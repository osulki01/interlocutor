# Standard libraries
import datetime

# Third party libraries
import pandas as pd
import pytest

# Internal imports
from interlocutor.database import postgresql


def test_execute_database_operation():
    """SQL command successfully executed on database."""

    db_connection = postgresql.DatabaseConnection(environment='stg')

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


def test_get_dataframe_demands_correct_arguments():
    """Exceptions or warnings should be raised if an incorrect combination of arguments is provided."""

    db_connection = postgresql.DatabaseConnection(environment='stg')

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

    db_connection = postgresql.DatabaseConnection(environment='stg')

    expected = pd.DataFrame(
        columns=['example_integer', 'example_string', 'example_timestamp'],
        data=[[1, 'First value', '2020-01-21T01:53:00Z'], [2, 'Second value', '2020-07-16T03:31:00Z']]
    )

    expected['example_timestamp'] = pd.to_datetime(expected['example_timestamp'], format='%Y-%m-%dT%H:%M:%SZ')

    actual = db_connection.get_dataframe(schema="testing_schema", table_name="testing_table")

    pd.testing.assert_frame_equal(left=actual, right=expected)


def test_get_dataframe_from_query():
    """All data from a table can be retrieved by providing a query."""

    db_connection = postgresql.DatabaseConnection(environment='stg')

    expected = pd.DataFrame(columns=['example_integer', 'example_string'], data=[[1, 'First value']])

    actual = db_connection.get_dataframe(
        query="SELECT example_integer, example_string FROM testing_schema.testing_table WHERE example_integer = 1;"
    )

    pd.testing.assert_frame_equal(left=actual, right=expected)


def test_get_dataframe_from_query_with_parameters():
    """All data from a table can be retrieved by providing a parameterised query."""

    db_connection = postgresql.DatabaseConnection(environment='stg')

    expected = pd.DataFrame(columns=['example_integer', 'example_string'], data=[[1, 'First value']])

    actual = db_connection.get_dataframe(
        query=("SELECT example_integer, example_string "
               "FROM testing_schema.testing_table WHERE example_integer = %(id)s;"),
        query_params={'id': 1}
    )

    pd.testing.assert_frame_equal(left=actual, right=expected)


def test_upload_dataframe():
    """Dataframe is successfully written to a permanent postgres table."""

    db_connection = postgresql.DatabaseConnection(environment='stg')

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
