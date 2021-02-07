"""Testing for crawling The i website and download article metadata/content."""

# Standard libraries
import os

# Third party libraries
import pandas as pd
import pytest
import requests

# Internal imports
from interlocutor.database import postgresql
from interlocutor.get_data import i_news


class MockAllColumnistsHomepage:
    """Mock a request call to the i News page which lists its columnists."""

    def __init__(self):
        self.status_code = 200

        script_directory = os.path.dirname(os.path.abspath(__file__))

        with open(file=f'{script_directory}/mock_i_news_columnists_homepage.html', mode='rb') as mock_page:
            self.content = mock_page.read()


def mock_all_columnists_homepage(url=None):
    """Mock the HTTP request to the page listing the paper's columnists."""

    return MockAllColumnistsHomepage()


class MockSpecificColumnistHomepage:
    """Mock a request call to the i News page with a specific columnist's homepage."""

    def __init__(self):
        self.status_code = 200

        script_directory = os.path.dirname(os.path.abspath(__file__))

        with open(file=f'{script_directory}/mock_i_news_author_homepage.html', mode='rb') as mock_page:
            self.content = mock_page.read()


def mock_specific_columnist_homepage(url=None):
    """Mock the HTTP request to the homepage of a specific columnist."""

    return MockSpecificColumnistHomepage()


def test_get_columnist_homepages(monkeypatch):
    """The name of columnists is successfully retrieved alongside their homepage."""

    expected_columnists = {
        'Fiona Mountford': 'https://inews.co.uk/author/fiona-mountford',
        'Sarah Carson': 'https://inews.co.uk/author/sarah-carson',
        'Ayesha Hazarika': 'https://inews.co.uk/author/ayesha-hazarika',
        'Alexander McCall Smith': 'https://inews.co.uk/author/alexander-mccallsmith',
        'Simon Kelner': 'https://inews.co.uk/author/simon-kelner',
        'Poorna Bell': 'https://inews.co.uk/author/poorna-bell'
    }

    monkeypatch.setattr(requests, 'get', mock_all_columnists_homepage)

    article_downloader = i_news.ArticleDownloader()
    actual_columnists = article_downloader._get_columnist_homepages()

    assert actual_columnists == expected_columnists


def test_get_recent_article_links(monkeypatch):
    """Recent articles published by a columnist are extracted from their homepage."""

    expected_links = [
        'https://inews.co.uk/opinion/lockdown-weight-gain-diet-culture-intuitive-eating-730258',
        'https://inews.co.uk/opinion/columnists/holidays-covid-19-summer-matt-hancock-restrictions-836796',
        'https://inews.co.uk/opinion/columnists/dry-january-continue-february-sobriety-alcohol-moderation-lessons'
        '-856131',
        'https://inews.co.uk/opinion/columnists/cuffing-season-online-dating-covid-19-single-lockdown-688149',
        'https://inews.co.uk/opinion/columnists/covid-rage-diary-processing-anger-grief-pandemic-827646',
        'https://inews.co.uk/opinion/columnists/covid-19-finances-budget-complusive-spending-846420',
        'https://inews.co.uk/opinion/my-mum-spoke-to-me-about-whether-i-wanted-children-just-before-my-40th-she-was'
        '-sensitive-and-open-771260',
        'https://inews.co.uk/opinion/living-alone-in-lockdown-was-difficult-the-government-needs-to-do-more-to-help'
        '-people-like-me-cope-656799',
        'https://inews.co.uk/opinion/covid-19-long-term-symptoms-fatigue-nausea-651571',
        'https://inews.co.uk/opinion/lets-learn-from-2008-and-include-mental-health-care-in-the-recession-recovery'
        '-plan-578338'
    ]

    monkeypatch.setattr(requests, 'get', mock_specific_columnist_homepage)

    article_downloader = i_news.ArticleDownloader()
    actual_links = article_downloader._get_recent_article_links(homepage='mock_url_so_required_argument_is_given')

    assert sorted(actual_links) == sorted(expected_links)


@pytest.mark.integration
def test_record_columnist_home_pages(monkeypatch):
    """Columnist names and their home page are pulled correctly and stored in postgres."""

    # Set up downloader but overwrite crawler with mock data
    article_downloader = i_news.ArticleDownloader()
    monkeypatch.setattr(requests, 'get', mock_all_columnists_homepage)

    article_downloader.record_columnist_home_pages()

    # Retrieve the table to see if it was populated correctly
    db_connection = postgresql.DatabaseConnection()
    db_connection._create_connection()

    with db_connection._conn.cursor() as curs:
        curs.execute('SELECT * FROM i_news.columnists;')

        table_tuples = curs.fetchall()
        actual_columnists = pd.DataFrame(table_tuples, columns=['columnist', 'homepage'])

        # Tidy up and return table to its original form
        curs.execute("TRUNCATE TABLE i_news.columnists;")
        curs.execute("COPY i_news.columnists FROM '/staging_data/i_news.columnists.csv' WITH CSV HEADER;")

        db_connection._conn.commit()

    db_connection._close_connection()

    expected_columnists = pd.DataFrame(data={
        'columnist': ['Fiona Mountford', 'Sarah Carson', 'Ayesha Hazarika', 'Alexander McCall Smith', 'Simon Kelner',
                      'Poorna Bell'],
        'homepage': ['https://inews.co.uk/author/fiona-mountford',
                     'https://inews.co.uk/author/sarah-carson',
                     'https://inews.co.uk/author/ayesha-hazarika',
                     'https://inews.co.uk/author/alexander-mccallsmith',
                     'https://inews.co.uk/author/simon-kelner',
                     'https://inews.co.uk/author/poorna-bell']
    })

    pd.testing.assert_frame_equal(actual_columnists, expected_columnists)


@pytest.mark.integration
def test_record_columnists_recent_article_links():
    """The URLs of articles by i News columnists are successfully extracted and stored in database."""

    # Inspect database table before it is populated
    db_connection = postgresql.DatabaseConnection()
    db_connection._create_connection()

    with db_connection._conn.cursor() as curs:
        curs.execute('SELECT * FROM i_news.columnist_article_links;')

        table_tuples = curs.fetchall()
        table_before_extracting = pd.DataFrame(table_tuples, columns=['columnist', 'article_id', 'url'])

        db_connection._conn.commit()

    # Run scraper and inspect database table after it is populated
    article_downloader = i_news.ArticleDownloader()
    article_downloader.record_columnists_recent_article_links()

    with db_connection._conn.cursor() as curs:
        curs.execute('SELECT * FROM i_news.columnist_article_links;')

        table_tuples = curs.fetchall()
        table_after_extracting = pd.DataFrame(table_tuples, columns=['columnist', 'article_id', 'url'])

        # Tidy up and return table to its original form
        original_data = pd.read_csv('Docker/db/staging_data/i_news.columnist_article_links.csv')
        original_urls = original_data['url'].values

        curs.execute(
            query='DELETE FROM i_news.columnist_article_links WHERE url NOT IN %(original_urls)s',
            vars={'original_urls': tuple(original_urls)}
        )

        db_connection._conn.commit()

    # The website page evolves over time, so perform checks which do not depend on the actual URLs

    # New rows have been added
    assert table_after_extracting.shape[0] > table_before_extracting.shape[0]

    # Appropriate URLs have been scraped
    assert all(
        url.startswith(('https://inews.co.uk/opinion/', 'https://inews.co.uk/culture/'))
        for url in table_after_extracting['url'].values
    )

    # Only links for the relevant columnists (stored in i_news.columnists) have been pulled
    df_expected_columnists = pd.read_csv('Docker/db/staging_data/i_news.columnists.csv')
    expected_columnists = df_expected_columnists['columnist'].unique().tolist()

    assert table_after_extracting['columnist'].unique().tolist() == expected_columnists
