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


class MockColumnistHomepage:
    """Mock a stripped down version of the Daily Mail page which lists its columnists."""

    def __init__(self):
        self.status_code = 200

        script_directory = os.path.dirname(os.path.abspath(__file__))

        with open(file=f'{script_directory}/mock_daily_mail_columnists_homepage.html', mode='rb') as mock_page:
            self.content = mock_page.read()


def mock_columnist_homepage(url=None):
    """Mock the HTTP request to the page listing the paper's columnists."""

    return MockColumnistHomepage()


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

    monkeypatch.setattr(requests, 'get', mock_columnist_homepage)

    article_downloader = daily_mail.ArticleDownloader()
    actual_columnists = article_downloader._get_columnist_homepages()

    assert actual_columnists == expected_columnists


@pytest.mark.integration
def test_record_columnist_home_pages(monkeypatch):
    """Columnist names and their home page are pulled correctly and stored in postgres."""

    # Set up downloader but overwrite crawler with mock data
    article_downloader = daily_mail.ArticleDownloader()
    monkeypatch.setattr(requests, 'get', mock_columnist_homepage)

    article_downloader.record_columnist_home_pages()

    # Retrieve the table to see if it was populated correctly
    db_connection = postgresql.DatabaseConnection()
    db_connection._create_connection()

    with db_connection._conn.cursor() as curs:
        curs.execute('SELECT * FROM daily_mail.columnists;')

        table_tuples = curs.fetchall()
        actual_columnists = pd.DataFrame(table_tuples, columns=['columnist', 'homepage'])

        # Tidy up and delete newly inserted rows
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
