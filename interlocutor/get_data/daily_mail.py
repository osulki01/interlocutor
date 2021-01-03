"""Crawl the Daily Mail website and download article metadata/content."""

# Standard libraries
import hashlib
import time
from typing import Dict, List, Tuple
from urllib import parse

# Third party libraries
from bs4 import BeautifulSoup
import pandas as pd
import requests
import tqdm

# Internal imports
from interlocutor.database import postgresql


class ArticleDownloader:
    """
    Crawl the Daily Mail website and capture information about the articles in its Columnists section.
    """

    def __init__(self):

        self._base_url = 'https://www.dailymail.co.uk'
        self._columnist_section_url = 'https://www.dailymail.co.uk/columnists/index.html'

        self._db_connection = postgresql.DatabaseConnection()

    def _get_article_title_and_content(self, url) -> Tuple[str, str]:
        """

        Parameters
        ----------
        url : str
            URL of the the Daily Mail article which a get request will be sent to.

        Returns
        -------
        tuple
            Title of article and its text content.
        """

        article_page = requests.get(url)

        article_soup = BeautifulSoup(markup=article_page.content, features="html.parser")

        # Extract title, which needs parsing as it follows convention '<Author>: <Title> | Daily Mail Online'
        title = article_soup.find("title").getText()
        title = title.split(': ')[-1]
        title = title.replace(' | Daily Mail Online', '').strip()

        # Extract the text content
        article_body = article_soup.find("div", {"itemprop": "articleBody"})
        raw_content = article_body.findAll(attrs={'class': 'mol-para-with-font'})
        processed_content = []

        for line in raw_content:
            processed_content.append(line.text)

        return title, ' '.join(processed_content)

    def _get_columnist_homepages(self) -> Dict[str, str]:
        """
        Scrape the Daily Mail page which lists their columnists and contains a link to their homepage.

        Returns
        -------
        dict
            Key: Columnist name, Value: URL for columnist's homepage.
        """

        columnists_homepage = requests.get(self._columnist_section_url)

        columnists_homepage_soup = BeautifulSoup(markup=columnists_homepage.content, features="html.parser")

        columnist_sections = columnists_homepage_soup.findAll(name="div", attrs={"class": "debate item"})

        columnists = {}

        for section in columnist_sections:
            author_home_pages = section.findAll(name="a", attrs={"class": "js-tl"})

            for author_page in author_home_pages:
                # Map the name of the author to their home page
                url = author_page.attrs['href']
                # Replace relative urls with absolute
                full_url = parse.urljoin(base=self._base_url, url=url)

                # Only save if their homepage contains regularly structured Daily Mail articles rather than RightMinds
                # blogs (which do not start with the usual base_url)
                if full_url.startswith(self._base_url):
                    columnists[author_page.text.title().strip()] = full_url

        return columnists

    def _get_recent_article_links(self, homepage: str) -> List[str]:
        """
        Extracts the links to recent articles published by a columnist.

        Parameters
        ----------
        homepage : str
            URL to the columnist's homepage.

        Returns
        -------
        list
            URLs for the most recent articles by columnist.
        """

        columnist_homepage = requests.get(homepage)

        parsed_homepage = BeautifulSoup(markup=columnist_homepage.content, features="html.parser")

        articles_section = parsed_homepage.find("div", {"class": "columnist-archive-page link-box linkro-darkred"})

        potential_articles = articles_section.findAll(name="a")

        article_links = []

        for link in potential_articles:

            href = link.attrs['href']

            # Ignore links to pages which simply show more articles further back in time
            if 'pageOffset' not in href:
                article_links.append(parse.urljoin(base=self._base_url, url=href))

        # Avoid duplicates
        return list(set(article_links))

    def record_columnist_home_pages(self) -> None:
        """
        Crawl the name of columnists and their homepage, then write to postgres.
        """

        columnists_and_pages = self._get_columnist_homepages()

        df_columnists_and_pages = pd.DataFrame.from_dict(data=columnists_and_pages, orient='index').reset_index()
        df_columnists_and_pages.columns = ['columnist', 'homepage']

        self._db_connection.upload_new_data_only_to_existing_table(
            dataframe=df_columnists_and_pages,
            table_name='columnists',
            schema='daily_mail',
            id_column='columnist'
        )

    def record_columnists_recent_article_content(self) -> None:
        """
        For all of the articles in daily_mail.columnist_recent_article_links table, extract the text content of those
        articles and write to database.
        """

        # Find articles that have not already been scraped
        author_and_recent_article_links = self._db_connection.get_dataframe(
            query="""
            SELECT * 
            FROM daily_mail.columnist_recent_article_links 
            WHERE article_id NOT IN (SELECT id FROM daily_mail.recent_article_content);
            """
        )

        for url in tqdm.tqdm(
                desc='Daily Mail article content retrieved',
                iterable=author_and_recent_article_links['url'].values,
                total=len(author_and_recent_article_links['url'].values),
                unit=' article'
        ):
            title, content = self._get_article_title_and_content(url=url)

            data_for_database = pd.DataFrame(data={
                'id': [hashlib.md5(url.encode('utf-8')).hexdigest()],
                'url': [url],
                'title': [title],
                'content': [content]
            })

            self._db_connection.upload_new_data_only_to_existing_table(
                dataframe=data_for_database,
                table_name='recent_article_content',
                schema='daily_mail',
                id_column='id'
            )

    def record_columnists_recent_article_links(self) -> None:
        """
        For all of the columnists in daily_mail.columnists table, extract the links to recent articles published by each
        columnist and write to database.
        """

        authors_and_homepage = self._db_connection.get_dataframe(table_name='columnists', schema='daily_mail')
        authors_and_homepage = authors_and_homepage.to_dict(orient='records')

        for author_page in authors_and_homepage:

            author = author_page['columnist']
            homepage = author_page['homepage']

            article_urls = self._get_recent_article_links(homepage)
            hashed_urls = [hashlib.md5(val.encode('utf-8')).hexdigest() for val in article_urls]

            print(f'Gathering links for recent articles by Daily Mail columnist {author}')
            recent_articles = pd.DataFrame(data={
                'columnist': author,
                'article_id': hashed_urls,
                'url': article_urls
            })

            self._db_connection.upload_new_data_only_to_existing_table(
                dataframe=recent_articles,
                table_name='columnist_recent_article_links',
                schema='daily_mail',
                id_column='article_id'
            )

            # Be polite, do not bombard API with too many requests at once
            time.sleep(0.5)


if __name__ == '__main__':

    print('Initialising class for downloading article metadata and content from The Daily Mail')
    article_downloader = ArticleDownloader()

    print('Retrieving the names of columnists and their homepages')
    article_downloader.record_columnist_home_pages()

    print('Retrieving the links to recent articles published by columnists')
    article_downloader.record_columnists_recent_article_links()

    print('Retrieving the text content of recent articles')
    article_downloader.record_columnists_recent_article_content()
