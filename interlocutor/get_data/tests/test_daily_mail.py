"""Testing crawling the Daily Mail website and download article metadata/content."""

# Standard libraries
import os
import re

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


class MockArticlePage:
    """Mock a request call to a Daily Mail article."""

    def __init__(self):
        self.status_code = 200

        script_directory = os.path.dirname(os.path.abspath(__file__))

        with open(file=f'{script_directory}/mock_daily_mail_article.html', mode='rb') as mock_page:
            self.content = mock_page.read()


def mock_article(url=None):
    """Mock the HTTP request to an individual article."""

    return MockArticlePage()


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


def test_get_article_title_and_content(monkeypatch):
    """Title and content of article are correctly scraped."""

    article_downloader = daily_mail.ArticleDownloader()
    monkeypatch.setattr(requests, 'get', mock_article)

    expected_title = 'Will these sinning saints be cancelled?'
    expected_content = """A couple of years ago, I wondered whether, if George Orwell were alive today, he would be 
    allowed to address a university, or to appear on television, or even to be published. Successive biographers have 
    found unsavoury incidents in the life of this great writer. As an officer in the Indian Imperial Police in Burma, 
    Orwell paid regular visits to the waterfront brothels of Rangoon and, later, to brothels in Morocco. A friend 
    remembered him saying that 'he found himself increasingly attracted by the young Arab girls'. Not long ago, 
    incriminating letters from Orwell's childhood friend, Jacintha Buddicom, were uncovered. These letters revealed 
    that the 15-year-old Orwell had attempted to rape Buddicom, but she had fought him off, and managed to escape 
    with a bruised hip and a torn skirt. In old age, Buddicom wrote of 'the public shame of being destroyed in a 
    classic book'. In Orwell's novel 1984, she had been portrayed as the doomed Julia. 'In the end he absolutely 
    destroys me, like a man in hobnailed boots stamping on a spider.' Furthermore, anyone reading his travel book 
    Down And Out In Paris And London will be staggered by Orwell's persistent anti-Semitism. On page 65 of my Penguin 
    edition, he fits as much offence as possible into 17 little words as he approvingly repeats an offensive proverb. 
    Were he alive today, Orwell would not be allowed to join any major — or minor — political party. He might even 
    find himself prosecuted for historic sex abuse, as well as incitement to racial hatred. Judging by what has 
    happened to today's authors, he might well find himself dropped by his publishers and cancelled by universities — 
    like historian Dr David Starkey, who caused a furore by making offensive remarks about slavery. He later 
    apologised. However, Orwell remains a liberal icon. Long sanctified by all shades of the political divide, 
    and now safely dead, his sins have been forgiven. His works are all still published, taught in schools and 
    revered the world over. And so they should be: left to themselves, readers can easily distinguish between the 
    virtues and vices of any given author. But what of another great secular saint, whose 80th anniversary was 
    celebrated this year? Throughout his youth, he was anti-Semitic, once loudly complaining that 'Hitler should have 
    finished the job'. Historic film footage exists of this icon performing the Nazi salute on the balcony of a town 
    hall in the early 1960s. A colleague remembered him in a drinking den, where 'he took a dislike to a 
    Semitic-featured piano player named Reuben, who seemed like a pleasant enough gentleman to me. 'While Reuben 
    manfully continued to jangle the ivories, he . . . persisted in disrupting the performance with [anti-Semitic] 
    taunts. In the end the poor fellow was reduced to tears.' He was routinely rude about foreigners. Talking to a 
    journalist in 1966, he spoke of his plans to send his little boy to the French Lycee in London. 'Seems the only 
    place for him in his position. I feel sorry for him, though. I couldn't stand ugly people even when I was five. 
    Lots of the ugly ones are foreign, aren't they?' He could be deeply abusive to young women. On a concert tour in 
    1962, one of his colleagues recalled a knock on the door of their shared dressing room. 'A girl kept asking if 
    she could come in. In his usual way, he told her to f*** off, but she kept knocking and asking to come in. He 
    said, 'Well, you can come in if you take all your clothes off.' So he let her in and she stood there all shy and 
    then he said, 'Well, get your clothes off then.' The poor girl — she was a trainee nurse — duly stripped off and 
    she was stark naked when the theatre manager walked in, and then promptly walked out again. It was typical. He 
    could be as crude and coarse as they come.' Forty years since his death, this man is universally regarded as a 
    crusader for peace and justice, as well as a towering creative genius. The National Trust conducts guided tours 
    of his childhood home and, so far as I know, has not yet issued an apology for his historic crimes. Who is he, 
    and how has he got away with it? These are questions I will try to answer in my next column. """

    actual_title, actual_content = article_downloader._get_article_title_and_content(
        url='mock_url_so_required_argument_is_given'
    )

    # Ensure we only compare content, not whether whitespace means the two strings are not the same
    multiple_consecutive_whitespace = re.compile(r"\s+")

    actual_content_no_multi_space = multiple_consecutive_whitespace.sub(" ", actual_content).strip()
    expected_content_no_multi_space = multiple_consecutive_whitespace.sub(" ", expected_content).strip()

    assert actual_title == expected_title
    assert actual_content_no_multi_space == expected_content_no_multi_space


def test_get_columnist_homepages(monkeypatch):
    """The name of columnists is successfully retrieved alongside their homepage."""

    expected_columnists = {
        'Baz Bamigboye': 'https://www.dailymail.co.uk/tvshowbiz/columnist-1000601/Baz-Bamigboye-Daily-Mail.html',
        'Craig Brown': 'https://www.dailymail.co.uk/home/books/columnist-1003951/Craig-Brown-Daily-Mail.html',
        'Alex Brummer': 'https://www.dailymail.co.uk/news/columnist-1001421/Alex-Brummer-Daily-Mail.html',
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
        'Andrew Pierce': 'https://www.dailymail.co.uk/news/columnist-1041755/Andrew-Pierce-The-Mail-Sunday.html',
        'Amanda Platell': 'https://www.dailymail.co.uk/news/columnist-463/Amanda-Platell-The-Daily-Mail.html',
        'Martin Samuel': 'https://www.dailymail.co.uk/sport/columnist-1020688/Martin-Samuel-Sport-Daily-Mail.html',
        'Ruth Sunderland': 'https://www.dailymail.co.uk/columnists/columnist-1072434/Ruth-Sunderland-Daily-Mail.html',
        'Tom Utley': 'https://www.dailymail.co.uk/news/columnist-1000961/Tom-Utley-Daily-Mail.html',
        'Sarah Vine': 'https://www.dailymail.co.uk/debate/columnist-1082216/Sarah-Vine-Daily-Mail.html',
        'Peter Hitchens': 'https://www.dailymail.co.uk/debate/columnist-224/Peter-Hitchens-The-Mail-Sunday.html',
        'Liz Jones': 'https://www.dailymail.co.uk/mailonsunday/columnist-1074669/Liz-Jones-Column-The-Mail-Sunday.html',
        'Black Dog': 'https://www.dailymail.co.uk/mailonsunday/columnist-249/Black-Dog-The-Mail-Sunday.html',
        'Oliver Holt': 'https://www.dailymail.co.uk/sport/columnist-1098989/Oliver-Holt-Mail-Sunday.html',
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
                      'Black Dog', 'Oliver Holt'],
        'homepage': ['https://www.dailymail.co.uk/tvshowbiz/columnist-1000601/Baz-Bamigboye-Daily-Mail.html',
                     'https://www.dailymail.co.uk/home/books/columnist-1003951/Craig-Brown-Daily-Mail.html',
                     'https://www.dailymail.co.uk/news/columnist-1001421/Alex-Brummer-Daily-Mail.html',
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
                     'https://www.dailymail.co.uk/news/columnist-1041755/Andrew-Pierce-The-Mail-Sunday.html',
                     'https://www.dailymail.co.uk/news/columnist-463/Amanda-Platell-The-Daily-Mail.html',
                     'https://www.dailymail.co.uk/sport/columnist-1020688/Martin-Samuel-Sport-Daily-Mail.html',
                     'https://www.dailymail.co.uk/columnists/columnist-1072434/Ruth-Sunderland-Daily-Mail.html',
                     'https://www.dailymail.co.uk/news/columnist-1000961/Tom-Utley-Daily-Mail.html',
                     'https://www.dailymail.co.uk/debate/columnist-1082216/Sarah-Vine-Daily-Mail.html',
                     'https://www.dailymail.co.uk/debate/columnist-224/Peter-Hitchens-The-Mail-Sunday.html',
                     'https://www.dailymail.co.uk/mailonsunday/columnist-1074669/Liz-Jones-Column-The-Mail-Sunday.html',
                     'https://www.dailymail.co.uk/mailonsunday/columnist-249/Black-Dog-The-Mail-Sunday.html',
                     'https://www.dailymail.co.uk/sport/columnist-1098989/Oliver-Holt-Mail-Sunday.html']
    })

    pd.testing.assert_frame_equal(actual_columnists, expected_columnists)


@pytest.mark.integration
def test_record_columnists_recent_article_content():
    """
    The content of articles whose links are stored in the daily_mail.columnist_recent_article_links table,
    is extracted and stored in daily_mail.recent_article_content
    """

    article_downloader = daily_mail.ArticleDownloader()
    article_downloader.record_columnists_recent_article_content()

    # Inspect target table to check it was populated correctly with link from daily_mail.columnist_recent_article_links
    # which has not been scraped yet
    db_connection = postgresql.DatabaseConnection()
    db_connection._create_connection()

    with db_connection._conn.cursor() as curs:
        curs.execute('SELECT * FROM daily_mail.recent_article_content;')

        table_tuples = curs.fetchall()
        actual_table = pd.DataFrame(table_tuples, columns=['url', 'title', 'content'])

        db_connection._conn.commit()

        # Tidy up and delete newly inserted rows those that don't exist in the staging data
        # Docker/db/staging_data/daily_mail.recent_article_content.csv)
        curs.execute("""DELETE FROM daily_mail.recent_article_content WHERE url <> 
        'https://www.dailymail.co.uk/tvshowbiz/article-9041823/BAZ-BAMIGBOYE-day-Carey-Mulligan-thought-kill-Ralph-Fiennes.html';
        """)

        db_connection._conn.commit()

    script_directory = os.path.dirname(os.path.abspath(__file__))
    expected_table = pd.read_csv(
        f'{script_directory}/daily_mail__record_columnists_recent_article_content__expected_output.csv'
    )

    pd.testing.assert_frame_equal(actual_table, expected_table)


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

    # The website page evolves over time, so perform checks which do not depend on the actual URLs

    # New rows have been added
    assert table_after_extracting.shape[0] > table_before_extracting.shape[0]

    # Appropriate URLs have been scraped
    assert all(url.startswith('https://www.dailymail.co.uk/') for url in table_after_extracting['url'].values)

    # Only links for the relevant columnists have been pulled
    assert table_after_extracting['columnist'].unique().tolist() == ['Baz Bamigboye', 'Craig Brown']
