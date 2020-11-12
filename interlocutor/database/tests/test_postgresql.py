# Third party libraries
import pandas as pd
import pytest

# Internal imports
from interlocutor.database import postgresql


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
