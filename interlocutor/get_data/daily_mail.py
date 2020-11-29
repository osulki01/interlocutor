"""Crawl the Daily Mail website and download article metadata/content."""

# Standard libraries
from typing import Dict

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

        self._db_connection = postgresql.DatabaseConnection()
        self._columnist_section_url = 'https://www.dailymail.co.uk/columnists/index.html'

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
            author_home_pages = section.findAll("a", {"class": "js-tl"})

            for author_page in author_home_pages:
                # Map the name of the author to their home page
                url = author_page.attrs['href']
                # Populate relative urls
                full_url = f'https://www.dailymail.co.uk{url}' if url.startswith('/') else url

                columnists[author_page.text.title().strip()] = full_url

        return columnists

    def get_columnist_recent_articles(self):
        """

        Returns
        -------

        """

        pass

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
