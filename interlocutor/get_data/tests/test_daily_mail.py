"""Testing crawling the Daily Mail website and download article metadata/content."""

# Standard libraries
import os

# Third party libraries
import pandas as pd
import pytest
import requests

# Internal imports
from interlocutor.database import postgresql
from interlocutor.get_data import daily_mail


class MockAllColumnistsHomepage:
    """Mock a request call to the Daily Mail page which lists its columnists."""

    def __init__(self):
        self.status_code = 200

        script_directory = os.path.dirname(os.path.abspath(__file__))

        with open(file=f'{script_directory}/mock_daily_mail_columnists_homepage.html', mode='rb') as mock_page:
            self.content = mock_page.read()


def mock_all_columnists_homepage(url=None):
    """Mock the HTTP request to the page listing the paper's columnists."""

    return MockAllColumnistsHomepage()


class MockSpecificColumnistHomepage:
    """Mock a request call to the Daily Mail page with a specific columnist's homepage."""

    def __init__(self):
        self.status_code = 200

        script_directory = os.path.dirname(os.path.abspath(__file__))

        with open(file=f'{script_directory}/mock_daily_mail_author_homepage.html', mode='rb') as mock_page:
            self.content = mock_page.read()


def mock_specific_columnist_homepage(url=None):
    """Mock the HTTP request to the homepage of a specific columnist."""

    return MockSpecificColumnistHomepage()


def test_get_columnist_homepages(monkeypatch):
    """The name of columnists is successfully retrieved alongside their homepage."""

    expected_columnists = {
        'Baz Bamigboye': 'https://www.dailymail.co.uk/tvshowbiz/columnist-1000601/Baz-Bamigboye-Daily-Mail.html',
        'Craig Brown': 'https://www.dailymail.co.uk/home/books/columnist-1003951/Craig-Brown-Daily-Mail.html',
        'Alex Brummer': 'https://brummerblog.dailymail.co.uk/',
        'Stephen Glover': 'https://www.dailymail.co.uk/news/columnist-244/Stephen-Glover-Daily-Mail.html',
        'Richard Kay': 'https://www.dailymail.co.uk/news/columnist-230/Richard-Kay-Daily-Mail.html',
        'Ephraim Hardcastle': 'https://www.dailymail.co.uk/news/columnist-250/Ephraim-Hardcastle-Daily-Mail.html',
        'Sebastian Shakespeare': 'https://www.dailymail.co.uk/news/columnist-1092116/Sebastian-Shakespeare-Daily-Mail'
                                 '.html',
        'Max Hastings': 'https://www.dailymail.co.uk/news/columnist-464/Max-Hastings-Daily-Mail.html',
        'Dominic Lawson': 'https://www.dailymail.co.uk/columnists/columnist-1083636/Dominic-Lawson-Daily-Mail.html',
        'Richard Littlejohn': 'https://www.dailymail.co.uk/news/columnist-322/Richard-Littlejohn-Daily-Mail.html',
        'Peter Mckay': 'https://www.dailymail.co.uk/news/columnist-227/Peter-McKay-Daily-Mail.html',
        'Jan Moir': 'https://www.dailymail.co.uk/debate/columnist-1012602/Jan-Moir-Daily-Mail.html',
        'Bel Mooney': 'https://www.dailymail.co.uk/femail/columnist-465/Bel-Mooney-Daily-Mail.html',
        'Andrew Pierce': 'https://pierceblog.dailymail.co.uk/',
        'Amanda Platell': 'https://www.dailymail.co.uk/news/columnist-463/Amanda-Platell-The-Daily-Mail.html',
        'Martin Samuel': 'https://www.dailymail.co.uk/sport/columnist-1020688/Martin-Samuel-Sport-Daily-Mail.html',
        'Ruth Sunderland': 'https://www.dailymail.co.uk/columnists/columnist-1072434/Ruth-Sunderland-Daily-Mail.html',
        'Tom Utley': 'https://www.dailymail.co.uk/news/columnist-1000961/Tom-Utley-Daily-Mail.html',
        'Sarah Vine': 'https://www.dailymail.co.uk/debate/columnist-1082216/Sarah-Vine-Daily-Mail.html',
        'Peter Hitchens': 'https://hitchensblog.mailonsunday.co.uk/',
        'Liz Jones': 'https://www.dailymail.co.uk/mailonsunday/columnist-1074669/Liz-Jones-Column-The-Mail-Sunday.html',
        'Black Dog': 'https://www.dailymail.co.uk/mailonsunday/columnist-249/Black-Dog-The-Mail-Sunday.html',
        'Oliver Holt': 'https://www.dailymail.co.uk/sport/columnist-1098989/Oliver-Holt-Mail-Sunday.html',
        'Chapman & Co': 'https://chapman.dailymail.co.uk/',
        'Steve Doughty': 'https://doughtyblog.dailymail.co.uk/',
        'Adrian Hilton': 'https://hiltonblog.dailymail.co.uk/',
        'Mary Ellen Synon': 'https://synonblog.dailymail.co.uk/',
        'Stephen Wright': 'https://wrightblog.dailymail.co.uk/',
    }

    monkeypatch.setattr(requests, 'get', mock_all_columnists_homepage)

    article_downloader = daily_mail.ArticleDownloader()
    actual_columnists = article_downloader._get_columnist_homepages()

    assert actual_columnists == expected_columnists


def test_get_recent_article_links(monkeypatch):
    """Recent articles published by a columnist are extracted from their homepage."""

    expected_links = [
        'https://www.dailymail.co.uk/debate/article-8801931/PETER-HITCHENS-Save-democracy-vote-None-Party.html',
        'https://www.dailymail.co.uk/debate/article-8726051/PETER-HITCHENS-Government-wading-swamp-despotism-one'
        '-muzzle-time.html',
        'https://www.dailymail.co.uk/debate/article-8776033/PETER-HITCHENS-Boris-great-idea-Burn-house-TWICE-rid'
        '-wasps-nest.html',
        'https://www.dailymail.co.uk/debate/article-8725993/PETER-HITCHENS-argues-extraditing-Julian-Assange'
        '-threatens-press-freedom.html',
        'https://www.dailymail.co.uk/debate/article-8901637/PETER-HITCHENS-dictators-taken-didnt-notice.html',
        'https://www.dailymail.co.uk/debate/article-8631149/PETER-HITCHENS-state-sponsored-panic-times-killed-people'
        '-Covid-did.html',
        'https://www.dailymail.co.uk/debate/article-8751075/PETER-HITCHENS-Johnson-Junta-nice-rest-home.html',
        'https://www.dailymail.co.uk/debate/article-8996599/PETER-HITCHENS-MPs-destroy-jobs-lose-theirs.html',
        'https://www.dailymail.co.uk/debate/article-8876105/PETER-HITCHENS-Lets-turn-time-didnt-mess-clocks.html',
        'https://www.dailymail.co.uk/debate/article-9021775/PETER-HITCHENS-20-years-time-Eton-just-pricey-Bog-Lane'
        '-comprehensive.html',
        'https://www.dailymail.co.uk/debate/article-8925171/PETER-HITCHENS-never-forgive-clowns-cancelled-Remembrance'
        '-Sunday.html',
        'https://www.dailymail.co.uk/debate/article-8654265/PETER-HITCHENS-holiday-wrecking-quarantines-worth'
        '-Government-havent-clue.html',
        'https://www.dailymail.co.uk/debate/article-8850927/PETER-HITCHENS-Britons-sentenced-slow-agonising-death-No'
        '-10s-panic-squad.html',
        'https://www.dailymail.co.uk/debate/article-8949379/PETER-HITCHENS-panicking-Prime-Minister-bankrupting'
        '-Britain.html',
        'https://www.dailymail.co.uk/debate/article-8631301/PETER-HITCHENS-Im-growing-new-beard-havent-felt'
        '-rebellious-1960s.html',
        'https://www.dailymail.co.uk/debate/article-8677727/PETER-HITCHENS-rant-BBC-Proms-make-slaves.html',
        'https://www.dailymail.co.uk/debate/article-8607739/PETER-HITCHENS-woman-said-IRA-right-kill-children-really'
        '-Baroness.html',
        'https://www.dailymail.co.uk/debate/article-8826683/PETER-HITCHENS-tries-force-apart-mourners-funeral-Ill'
        '-haunt-them.html',
        'https://www.dailymail.co.uk/debate/article-8973423/PETER-HITCHENS-doesnt-matter-vote-Greens-win.html',
        'https://www.dailymail.co.uk/debate/article-8701699/PETER-HITCHENS-Protest-against-new-State-Fear-banned.html',
    ]

    monkeypatch.setattr(requests, 'get', mock_specific_columnist_homepage)

    article_downloader = daily_mail.ArticleDownloader()
    actual_links = article_downloader._get_recent_article_links(homepage='mock_url_so_required_argument_is_given')

    assert sorted(actual_links) == sorted(expected_links)


@pytest.mark.integration
def test_record_columnist_home_pages(monkeypatch):
    """Columnist names and their home page are pulled correctly and stored in postgres."""

    # Set up downloader but overwrite crawler with mock data
    article_downloader = daily_mail.ArticleDownloader()
    monkeypatch.setattr(requests, 'get', mock_all_columnists_homepage)

    article_downloader.record_columnist_home_pages()

    # Retrieve the table to see if it was populated correctly
    db_connection = postgresql.DatabaseConnection()
    db_connection._create_connection()

    with db_connection._conn.cursor() as curs:
        curs.execute('SELECT * FROM daily_mail.columnists;')

        table_tuples = curs.fetchall()
        actual_columnists = pd.DataFrame(table_tuples, columns=['columnist', 'homepage'])

        # Tidy up and delete newly inserted rows
        # (those that don't exist in the staging data Docker/db/staging_data/daily_mail.columnists.csv)
        curs.execute("DELETE FROM daily_mail.columnists WHERE columnist NOT IN ('Baz Bamigboye', 'Craig Brown');")

        db_connection._conn.commit()

    db_connection._close_connection()

    expected_columnists = pd.DataFrame(data={
        'columnist': ['Baz Bamigboye', 'Craig Brown', 'Alex Brummer', 'Stephen Glover', 'Richard Kay',
                      'Ephraim Hardcastle', 'Sebastian Shakespeare', 'Max Hastings', 'Dominic Lawson',
                      'Richard Littlejohn', 'Peter Mckay', 'Jan Moir', 'Bel Mooney', 'Andrew Pierce', 'Amanda Platell',
                      'Martin Samuel', 'Ruth Sunderland', 'Tom Utley', 'Sarah Vine', 'Peter Hitchens', 'Liz Jones',
                      'Black Dog', 'Oliver Holt', 'Chapman & Co', 'Steve Doughty', 'Adrian Hilton', 'Mary Ellen Synon',
                      'Stephen Wright'],
        'homepage': ['https://www.dailymail.co.uk/tvshowbiz/columnist-1000601/Baz-Bamigboye-Daily-Mail.html',
                     'https://www.dailymail.co.uk/home/books/columnist-1003951/Craig-Brown-Daily-Mail.html',
                     'https://brummerblog.dailymail.co.uk/',
                     'https://www.dailymail.co.uk/news/columnist-244/Stephen-Glover-Daily-Mail.html',
                     'https://www.dailymail.co.uk/news/columnist-230/Richard-Kay-Daily-Mail.html',
                     'https://www.dailymail.co.uk/news/columnist-250/Ephraim-Hardcastle-Daily-Mail.html',
                     'https://www.dailymail.co.uk/news/columnist-1092116/Sebastian-Shakespeare-Daily-Mail.html',
                     'https://www.dailymail.co.uk/news/columnist-464/Max-Hastings-Daily-Mail.html',
                     'https://www.dailymail.co.uk/columnists/columnist-1083636/Dominic-Lawson-Daily-Mail.html',
                     'https://www.dailymail.co.uk/news/columnist-322/Richard-Littlejohn-Daily-Mail.html',
                     'https://www.dailymail.co.uk/news/columnist-227/Peter-McKay-Daily-Mail.html',
                     'https://www.dailymail.co.uk/debate/columnist-1012602/Jan-Moir-Daily-Mail.html',
                     'https://www.dailymail.co.uk/femail/columnist-465/Bel-Mooney-Daily-Mail.html',
                     'https://pierceblog.dailymail.co.uk/',
                     'https://www.dailymail.co.uk/news/columnist-463/Amanda-Platell-The-Daily-Mail.html',
                     'https://www.dailymail.co.uk/sport/columnist-1020688/Martin-Samuel-Sport-Daily-Mail.html',
                     'https://www.dailymail.co.uk/columnists/columnist-1072434/Ruth-Sunderland-Daily-Mail.html',
                     'https://www.dailymail.co.uk/news/columnist-1000961/Tom-Utley-Daily-Mail.html',
                     'https://www.dailymail.co.uk/debate/columnist-1082216/Sarah-Vine-Daily-Mail.html',
                     'https://hitchensblog.mailonsunday.co.uk/',
                     'https://www.dailymail.co.uk/mailonsunday/columnist-1074669/Liz-Jones-Column-The-Mail-Sunday.html',
                     'https://www.dailymail.co.uk/mailonsunday/columnist-249/Black-Dog-The-Mail-Sunday.html',
                     'https://www.dailymail.co.uk/sport/columnist-1098989/Oliver-Holt-Mail-Sunday.html',
                     'https://chapman.dailymail.co.uk/', 'https://doughtyblog.dailymail.co.uk/',
                     'https://hiltonblog.dailymail.co.uk/', 'https://synonblog.dailymail.co.uk/',
                     'https://wrightblog.dailymail.co.uk/']
    })

    pd.testing.assert_frame_equal(actual_columnists, expected_columnists)


@pytest.mark.integration
def test_record_columnists_recent_article_links():
    """The URLs for Daily Mail columnists are successfully extracted and stored in database."""

    # Inspect database table before it is populated
    db_connection = postgresql.DatabaseConnection()
    db_connection._create_connection()

    with db_connection._conn.cursor() as curs:
        curs.execute('SELECT * FROM daily_mail.columnist_recent_article_links;')

        table_tuples = curs.fetchall()
        table_before_extracting = pd.DataFrame(table_tuples, columns=['columnist', 'url'])

        db_connection._conn.commit()

    # Run scraper and inspect database table after it is populated
    article_downloader = daily_mail.ArticleDownloader()
    article_downloader.record_columnists_recent_article_links()

    with db_connection._conn.cursor() as curs:
        curs.execute('SELECT * FROM daily_mail.columnist_recent_article_links;')

        table_tuples = curs.fetchall()
        table_after_extracting = pd.DataFrame(table_tuples, columns=['columnist', 'url'])

        # Tidy up and delete newly inserted rows those that don't exist in the staging data
        # Docker/db/staging_data/daily_mail.columnist_recent_article_links.csv)
        curs.execute("""
        DELETE FROM daily_mail.columnist_recent_article_links
        WHERE url NOT IN (
          'https://www.dailymail.co.uk/tvshowbiz/article-9041823/BAZ-BAMIGBOYE-day-Carey-Mulligan-thought-kill-Ralph-Fiennes.html',
          'https://www.dailymail.co.uk/debate/article-9037459/CRAIG-BROWN-Crowns-bit-fishy-Lady-Anne-Chovy.html'
        );
        """)

        db_connection._conn.commit()

    # The page evolves over time, so perform checks which do not depend on the actual URLs

    # New rows have been added
    assert table_after_extracting.shape[0] > table_before_extracting.shape[0]

    # Appropriate URLs have been scraped
    assert all(url.startswith('https://www.dailymail.co.uk/') for url in table_after_extracting['url'].values)

    # Only links for the relevant columnists have been pulled
    assert table_after_extracting['columnist'].unique().tolist() == ['Baz Bamigboye', 'Craig Brown']
