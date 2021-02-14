"""Crawl The i website and download article metadata/content."""

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
    Crawl The i website and capture information about the articles in its Columnists section.
    """

    def __init__(self):

        self._base_url = 'https://inews.co.uk/'
        self._columnist_section_url = 'https://inews.co.uk/category/opinion'
        self._db_connection = postgresql.DatabaseConnection()

    @staticmethod
    def _get_article_title_and_content(url) -> Tuple[str, str]:
        """
        Extract the title and content of an article based on its URL.

        Parameters
        ----------
        url : str
            URL of the the i News article which a get request will be sent to.

        Returns
        -------
        tuple
            Title of article and its text content.
        """

        article_page = requests.get(url)

        article_soup = BeautifulSoup(markup=article_page.content, features="html.parser")

        article_section = article_soup.find(name="article")

        title = article_section.find(name="h1", attrs={"class": "headline"}).getText()

        article_body = article_soup.find(name="div", attrs={"class": "article-padding article-content"})

        raw_content = article_body.findAll(name="p")
        processed_content = []

        for content in raw_content:

            # Check the paragraph is not exclusively a link to another article
            parent_element = content.findParent()
            if parent_element.name == 'div' and parent_element['class'] == ['inews__shortcode-readmore__text']:
                continue

            processed_content.append(content.text)

        return title, ' '.join(processed_content)

    def _get_columnist_homepages(self) -> Dict[str, str]:
        """
        Scrape the i News page which lists their columnists and contains a link to their homepage.

        Returns
        -------
        dict
            Key: Columnist name, Value: URL for columnist's homepage.
        """

        columnists_homepage = requests.get(self._columnist_section_url)

        columnists_homepage_soup = BeautifulSoup(markup=columnists_homepage.content, features="html.parser")

        columnist_section = columnists_homepage_soup.find(
            name="div",
            attrs={"class": "inews__post-section inews__post-section__cat-columnists"}
        )

        columnists = {}

        for individual_section in columnist_section.findAll(name="span", attrs={"class": "inews__post__category"}):

            author_and_url = individual_section.find(name="a")

            author = author_and_url.attrs['title']
            url = author_and_url.attrs['href']

            columnists[author] = url

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

        articles_section = parsed_homepage.find("div", {"class": "inews__main row"})

        potential_articles = articles_section.findAll(name="h2")

        article_links = []

        for article in potential_articles:

            link = article.find(name="a")
            url = link.attrs['href']

            if url.startswith(self._base_url):
                article_links.append(url)

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
            schema='i_news',
            id_column='columnist'
        )

    def record_columnists_recent_article_content(self) -> None:
        """
        For all of the articles in i_news.columnist_recent_article_links table, extract the text content of those
        articles and write to database.
        """

        # Find articles that have not already been scraped
        author_and_recent_article_links = self._db_connection.get_dataframe(
            query="""
            SELECT * 
            FROM i_news.columnist_article_links 
            WHERE article_id NOT IN (SELECT id FROM i_news.article_content);
            """
        )

        for url in tqdm.tqdm(
                desc='i News article content retrieved',
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
                table_name='article_content',
                schema='i_news',
                id_column='id'
            )

    def record_columnists_recent_article_links(self) -> None:
        """
        For all of the columnists in i_news.columnists table, extract the links to recent articles published by each
        columnist and write to database.
        """

        authors_and_homepage = self._db_connection.get_dataframe(table_name='columnists', schema='i_news')
        authors_and_homepage = authors_and_homepage.to_dict(orient='records')

        for author_page in authors_and_homepage:

            author = author_page['columnist']
            homepage = author_page['homepage']

            article_urls = self._get_recent_article_links(homepage)
            hashed_urls = [hashlib.md5(val.encode('utf-8')).hexdigest() for val in article_urls]

            print(f'Gathering links for recent articles by i News columnist {author}')
            recent_articles = pd.DataFrame(data={
                'columnist': author,
                'article_id': hashed_urls,
                'url': article_urls
            })

            self._db_connection.upload_new_data_only_to_existing_table(
                dataframe=recent_articles,
                table_name='columnist_article_links',
                schema='i_news',
                id_column='article_id'
            )

            # Be polite, do not bombard API with too many requests at once
            time.sleep(0.5)


if __name__ == '__main__':  # pragma: no cover (exclude from testing coverage report)

    print('Initialising class for downloading article metadata and content from i News')
    article_downloader = ArticleDownloader()

    print('Retrieving the names of columnists and their homepages')
    article_downloader.record_columnist_home_pages()

    print('Retrieving the links to recent articles published by columnists')
    article_downloader.record_columnists_recent_article_links()

    print('Retrieving the text content of recent articles')
    article_downloader.record_columnists_recent_article_content()
