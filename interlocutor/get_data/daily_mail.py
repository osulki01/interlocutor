"""Crawl the Daily Mail website and download article metadata/content."""

# Standard libraries
from typing import Dict, List
from urllib import parse

# Third party libraries
from bs4 import BeautifulSoup
import pandas as pd
import requests

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

    def record_columnists_recent_article_links(self) -> None:
        """
        For all of the columnists in daily_mail.columnists table, extract the links to recent articles published by each
        columnist and write to database..
        """

        authors_and_homepage = self._db_connection.get_dataframe(table_name='columnists', schema='daily_mail')
        authors_and_homepage = authors_and_homepage.to_dict(orient='records')

        for author_page in authors_and_homepage:

            author = author_page['columnist']
            homepage = author_page['homepage']

            print(f'Gathering links for recent articles by Daily Mail columnist {author}')
            recent_articles = pd.DataFrame(data={
                'columnist': author,
                'url': self._get_recent_article_links(homepage)
            })

            self._db_connection.upload_new_data_only_to_existing_table(
                dataframe=recent_articles,
                table_name='columnist_recent_article_links',
                schema='daily_mail',
                id_column='url'
            )


if __name__ == '__main__':

    article_downloader = ArticleDownloader()

    article_downloader.record_columnists_recent_article_links()
