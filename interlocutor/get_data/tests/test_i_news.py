"""Testing for crawling The i website and download article metadata/content."""

# Standard libraries
import os
import re

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


class MockArticlePage:
    """Mock a request call to a i News article."""

    def __init__(self):
        self.status_code = 200

        script_directory = os.path.dirname(os.path.abspath(__file__))

        with open(file=f'{script_directory}/mock_i_news_article.html', mode='rb') as mock_page:
            self.content = mock_page.read()


def mock_article(url=None):
    """Mock the HTTP request to an individual article."""

    return MockArticlePage()


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


def test_get_article_title_and_content(monkeypatch):
    """Title and content of article are correctly scraped."""

    article_downloader = i_news.ArticleDownloader()
    monkeypatch.setattr(requests, 'get', mock_article)

    expected_title = 'Captain Tom Moore embodied our great British fondness for doing benignly pointless things for ' \
                     'charity'
    expected_content = """We can all, I think, understand the great sense of loss that the country felt upon the sad 
    announcement this week of the death of Captain Sir Tom Moore, a man who achieved more in the months around his 
    hundredth birthday than most of us could ever accomplish in several lifetimes. He was a cheerful and stoic old 
    soldier, one of a dwindling band of survivors of the Greatest Generation, whose ‘tomorrow will be a good day’ 
    catchphrase managed the rare feat of sounding genuine rather than nicked from a Hallmark card. Yet I would 
    contest that there was even more to Captain Tom than has hitherto been eulogised. Surely one of the many factors 
    contributing to this lovely gentleman’s phenomenal appeal – both literal and metaphorical – was the fact that he 
    tapped into the great British fondness for, and famously generous response to, kindly people doing benignly 
    pointless things for a good cause. Cast your mind back to the Before Times of 2019 and earlier: wasn’t there 
    always someone encouraging you to pay a pound for charity to guess how many nails there were in a jar, 
    or asking you to sponsor their kids while they did 20 laps of their paddling pool? Some of my fondest childhood 
    memories come from the evening of Children in Need, a television event for which I maintain a huge and 
    unfashionable fondness (I memorably came home early from a first date a few years ago in order to watch Pudsey 
    Bear and his jolly team in their mid-November action. The fledgling relationship, perhaps unsurprisingly, 
    did not bloom). As I recall it, in the 1980s there were endless stories of people sitting in baths 
    of baked beans for hours/days/weeks on end in order to fundraise and the British public responded to the heroic 
    pointlessness of these challenges with philanthropic gusto. In fact, it seemed that the more esoteric the task, 
    the deeper our collective pockets became. I don’t ever remember one of our local community sponsored litter picks 
    – useful! Tidy! Fewer discarded takeaway cartons littering Epping Forest! – sparking anywhere near such an 
    enthusiastic response. Maybe it was just the children I hung out with growing up, but our sponsored silences 
    always managed to rake in the cash for charity. One of the myriad minor-key privations that we have suffered as a 
    country over this past time-out-of-time year is the chance to participate in acts of small-scale community 
    benevolence. There have been no Cub Scouts washing cars for charity, no Brownie cookie bakes, no school or 
    village fetes, which meant that my extraordinary run of luck at the bottle-jar tombola – a jar of elderly jam? 
    Yes please! – went untested in 2020. I shudder to think how many kindly-intentioned but nonetheless inedible 
    cakes went unmade and unsold last year, and thus how much money went unraised for all manner of good causes. No 
    wonder charities across the board are now reporting dwindling income and curtailed spheres of operation. One of 
    our most appealing national characteristics has long been our generosity; one of the most cherishable statistics 
    I know concerns the UK’s charitable giving in those terrible days immediately following the Boxing Day tsunami in 
    2004, when we as a country out-donated the rest of Europe put together. This instinctive kindness of ours has had 
    precious few outlets in the past year, which is why Captain Sir Tom and his laps of his garden struck such an 
    almighty chord. It is fervently to be hoped that as the vaccine roll-out gathers momentum across 2021, 
    so do announcements of fairs and festivals, bazaars and bring-and-buy sales, with their multiple opportunities to 
    chuck soggy sponges at teachers and vicars who stand gamely in the stocks in the name of raising funds for 
    skew-whiff church steeples across the land. We owe it to Captain Tom and his inspiring legacy to carry on walking 
    the path of resilient, unshowy generosity – and if we’re really good, there might even be a home-made scone with 
    some jam of dubious provenance to reward us for our efforts when we finish. That, or a relaxing bath of baked 
    beans to sooth aching muscles. """

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
def test_record_columnists_recent_article_content():
    """
    The content of articles whose links are stored in the i_news.columnist_article_links table,
    is extracted and stored in i_news.article_content
    """

    article_downloader = i_news.ArticleDownloader()
    article_downloader.record_columnists_recent_article_content()

    # Inspect target table to check it was populated correctly with link from i_news.columnist_recent_article_links
    # which has not been scraped yet
    db_connection = postgresql.DatabaseConnection()
    db_connection._create_connection()

    with db_connection._conn.cursor() as curs:
        curs.execute('SELECT * FROM i_news.article_content;')

        table_tuples = curs.fetchall()
        actual_table = pd.DataFrame(table_tuples, columns=['id', 'url', 'title', 'content'])

        db_connection._conn.commit()

        # Tidy up and return table to its original form
        original_data = pd.read_csv('Docker/db/staging_data/i_news.article_content.csv')
        original_urls = original_data['url'].values

        curs.execute(
            query='DELETE FROM i_news.article_content WHERE url NOT IN %(original_urls)s',
            vars={'original_urls': tuple(original_urls)}
        )

        db_connection._conn.commit()

    script_directory = os.path.dirname(os.path.abspath(__file__))
    expected_table = pd.read_csv(
        f'{script_directory}/i_news__record_columnists_recent_article_content__expected_output.csv'
    )

    pd.testing.assert_frame_equal(actual_table, expected_table)


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
