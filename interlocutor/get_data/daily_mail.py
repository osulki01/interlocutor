"""Crawl the Daily Mail website and download article metadata/content."""

# Standard libraries
import hashlib
import os
import time
from typing import Any, Dict, List, Union

# Third party libraries
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import requests
import tqdm

# Internal imports
from interlocutor.commons import commons
from interlocutor.database import postgresql


def get_columnist_homepages() -> Dict[str, str]:
    """
    Scrape the Daily Mail page which lists their columnists and contains a link to their homepage.

    Returns
    -------
    dict
        Key: Columnist name, Value: URL for columnist's homepage.
    """

    columnists_homepage = requests.get('https://www.dailymail.co.uk/columnists/index.html')

    columnists_homepage_soup = BeautifulSoup(markup=columnists_homepage.content, features="html.parser")

    columnist_sections = columnists_homepage_soup.findAll(name="div", attrs={"class": "debate item"})

    columnists = {}

    for section in columnist_sections:
        author_home_pages = section.findAll("a", {"class": "js-tl"})

        for author_page in author_home_pages:
            # Map the name of the author to their home page
            columnists[author_page.text.title()] = author_page.attrs['href']

    return columnists
