"""Testing crawling the Daily Mail website and download article metadata/content."""

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
        self.content = b"""
        <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
            "//www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
        <html>
           <head>
           </head>
           <body class="tlc-columnists columnistscolumnists mol-desktop aileron-font" id="columnists"
                itemid="/columnists/index.html" itemscope="" itemtype="//schema.org/CollectionPage">
              <div class="debate item">
                 <div class="editors-choice ccox link-ccox linkro-darkred" id="p-6"
                    data-track-module="llg-1001345^editors_choice">
                    <h3 class="bdrcc">DAILY MAIL COLUMNISTS </h3>
                    <ul class="cleared">
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl" href="/tvshowbiz/columnist-1000601/Baz-Bamigboye-Daily-Mail.html">
                                BAZ BAMIGBOYE</a>
                       </li>
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl" href="/home/books/columnist-1003951/Craig-Brown-Daily-Mail.html">
                                CRAIG BROWN</a>
                       </li>
                    </ul>
                 </div>
              </div>
              <script type="text/javascript">    DM.later(\'bundle\', function()
                {DM.has(\'p-6\', \'externalLinkTracker\');    });</script>
              <div class="debate item">
                 <div class="editors-choice ccox link-ccox linkro-darkred" id="p-45"
                 data-track-module="llg-1001682^editors_choice">
                    <h3 class="bdrcc">MAIL ON SUNDAY COLUMNISTS </h3>
                    <ul class="cleared">
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl" href="/debate/columnist-224/Peter-Hitchens-The-Mail-Sunday.html">
                                PETER HITCHENS</a>
                       </li>
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl"
                                    href="/mailonsunday/columnist-1074669/Liz-Jones-Column-The-Mail-Sunday.html">
                                    LIZ JONES</a>
                       </li>
                    </ul>
                 </div>
              </div>
              <script type="text/javascript">    DM.later(\'bundle\', function()
                {DM.has(\'p-45\', \'externalLinkTracker\');    });</script>
              <div class="debate item">
                 <div class="editors-choice ccox link-ccox linkro-darkred" id="p-54"
                    data-track-module="llg-1000543^editors_choice">
                    <h3 class="bdrcc">RIGHTMINDS BLOGGERS </h3>
                    <ul class="cleared">
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl" href="https://brummerblog.dailymail.co.uk/"
                                rel="nofollow noreferrer noopener" target="_blank">ALEX BRUMMER</a>
                       </li>
                       <li>
                          <div class="wocc">&nbsp;</div>
                                <a class="js-tl" href="https://chapman.dailymail.co.uk/"
                                rel="nofollow noreferrer noopener" target="_blank">CHAPMAN & CO</a>
                       </li>
                    </ul>
                 </div>
              </div>
           </body>
        </html>
        """


def mock_columnist_homepage(url=None):
    """Mock the HTTP request to the page listing the paper's columnists."""

    return MockColumnistHomepage()


def test_get_columnist_homepages(monkeypatch):
    """The name of columnists is successfully retrieved alongside their homepage."""

    expected_columnists = {
        'Baz Bamigboye': 'https://www.dailymail.co.uk/tvshowbiz/columnist-1000601/Baz-Bamigboye-Daily-Mail.html',
        'Craig Brown': 'https://www.dailymail.co.uk/home/books/columnist-1003951/Craig-Brown-Daily-Mail.html',
        'Peter Hitchens': 'https://www.dailymail.co.uk/debate/columnist-224/Peter-Hitchens-The-Mail-Sunday.html',
        'Liz Jones': 'https://www.dailymail.co.uk/mailonsunday/columnist-1074669/Liz-Jones-Column-The-Mail-Sunday.html',
        'Alex Brummer': 'https://brummerblog.dailymail.co.uk/',
        'Chapman & Co': 'https://chapman.dailymail.co.uk/'
    }

    monkeypatch.setattr(requests, 'get', mock_columnist_homepage)

    article_downloader = daily_mail.ArticleDownloader()
    actual_columnists = article_downloader._get_columnist_homepages()

    assert actual_columnists == expected_columnists


@pytest.mark.integration
def test_record_columnist_home_pages(monkeypatch):
    """Columnist names and their home page are pulled correctly and stored in postgres."""

    def mock_columnists():
        return {
            'Alex Brummer': 'https://www.dailymail.co.uk/news/columnist-1001421/Alex-Brummer-Daily-Mail.html',
            'Baz Bamigboye': 'https://www.dailymail.co.uk/tvshowbiz/columnist-1000601/Baz-Bamigboye-Daily-Mail.html',
            'Craig Brown': 'https://www.dailymail.co.uk/home/books/columnist-1003951/Craig-Brown-Daily-Mail.html'
        }

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
        'columnist': ['Baz Bamigboye', 'Craig Brown', 'Peter Hitchens', 'Liz Jones', 'Alex Brummer', 'Chapman & Co'],
        'homepage': [
            'https://www.dailymail.co.uk/tvshowbiz/columnist-1000601/Baz-Bamigboye-Daily-Mail.html',
            'https://www.dailymail.co.uk/home/books/columnist-1003951/Craig-Brown-Daily-Mail.html',
            'https://www.dailymail.co.uk/debate/columnist-224/Peter-Hitchens-The-Mail-Sunday.html',
            'https://www.dailymail.co.uk/mailonsunday/columnist-1074669/Liz-Jones-Column-The-Mail-Sunday.html',
            'https://brummerblog.dailymail.co.uk/',
            'https://chapman.dailymail.co.uk/'
        ]
    })

    pd.testing.assert_frame_equal(actual_columnists, expected_columnists)
