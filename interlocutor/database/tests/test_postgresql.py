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
        columns=['id', 'guardian_id', 'content_type', 'section_id', 'section_name', 'web_publication_timestamp',
                 'web_title', 'web_url', 'api_url', 'pillar_id', 'pillar_name'],
        data=[[1, 'politics/1990/nov/23/past.conservatives', 'article', 'commentisfree', 'Opinion',
               '1990-11-23T16:47:00Z', 'The Thatcher Years | Hugo Young',
               'https://www.theguardian.com/politics/1990/nov/23/past.conservatives',
               'https://content.guardianapis.com/politics/1990/nov/23/past.conservatives', 'pillar/opinion', 'Opinion'],
              [2, 'world/2002/feb/25/race.uk', 'article', 'commentisfree', 'Opinion',
               '2002-02-25T01:53:00Z', 'Gary Younge: Terms of abuse',
               'https://www.theguardian.com/world/2002/feb/25/race.uk',
               'https://content.guardianapis.com/world/2002/feb/25/race.uk', 'pillar/opinion', 'Opinion']]
    )

    expected['web_publication_timestamp'] = pd.to_datetime(
        expected['web_publication_timestamp'],
        format='%Y-%m-%dT%H:%M:%SZ'
    )

    actual = db_connection.get_dataframe(schema="the_guardian", table_name="article_metadata")

    pd.testing.assert_frame_equal(left=actual, right=expected)


def test_get_dataframe_from_query():
    """All data from a table can be retrieved by providing a query."""

    db_connection = postgresql.DatabaseConnection()

    expected = pd.DataFrame(
        columns=['id', 'guardian_id'],
        data=[[1, 'politics/1990/nov/23/past.conservatives']]
    )

    actual = db_connection.get_dataframe(
        query="SELECT id, guardian_id FROM the_guardian.article_metadata WHERE id = 1;"
    )

    pd.testing.assert_frame_equal(left=actual, right=expected)


def test_get_dataframe_from_query_with_parameters():
    """All data from a table can be retrieved by providing a parameterised query."""

    db_connection = postgresql.DatabaseConnection()

    expected = pd.DataFrame(
        columns=['id', 'guardian_id'],
        data=[[1, 'politics/1990/nov/23/past.conservatives']]
    )

    actual = db_connection.get_dataframe(
        query="SELECT id, guardian_id FROM the_guardian.article_metadata WHERE id = %(id)s;",
        query_params={'id': 1}
    )

    pd.testing.assert_frame_equal(left=actual, right=expected)
