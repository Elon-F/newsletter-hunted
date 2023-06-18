"""
Functions to help retrieve, parses, and stores every newsletter from Newsletterhunt.
"""
from os.path import isfile
from typing import Tuple

import bs4
import pandas as pd
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
from requests import get

MAX_RETRIES = 3

DF_COLUMNS = ["title", "name", "newsletter_url", "id", "date"]
NEWSLETTER_URL = "https://newsletterhunt.com/emails/{}"
STORAGE_LOCATION = "./newsletter.parquet"

SELECTORS = {
    "newsletter_name": 'p>a[href^="https://newsletterhunt.com/newsletters/"]',
    "title": 'div.min-w-0>h2',
    "date": 'div.min-w-0>p>time',  # requires looking at the content and converting

}


def parse_name(data: bs4.element):
    """
    Given the newsletter element, returns the newsletter's name and URL
    :return: Tuple(letter_url name, URL)
    """
    # Example: <a href="https://newsletterhunt.com/newsletters/money-stuff-by-matt-levine">Money Stuff by Matt Levine</a>
    tag_name = data.select_one(SELECTORS["newsletter_name"])
    return tag_name['href'], tag_name.contents[0].strip()


def parse_title(data: bs4.element):
    """
    Given the newsletter element, returns the title of the letter_url.
    :return: letter_url title
    """
    # Example: <h2 class="text-xl font-bold text-gray-500"> Money Stuff: Don’t Squeeze the Shorts </h2>
    tag_title = data.select_one(SELECTORS["title"])
    return tag_title.contents[0].strip()


def parse_date(data: bs4.element):
    """
    Given the newsletter element, returns the publication date of  the letter_url
    :return: publication date
    """
    tag_date = data.select_one(SELECTORS["date"])
    return tag_date.contents[0].strip()


def get_newsletter_info(newsletter_id: str or int):
    """
    Parses the relevant information about the letter_url of the given newsletter_id.
    :return: Tuple containing the source newsletters URL, newsletter name, letter_url title, letter_url publication date, and letter_url's ID.
    """
    url = NEWSLETTER_URL.format(newsletter_id)
    response = get(url)
    for i in range(MAX_RETRIES):
        if response.status_code == 200:
            break
        if response.status_code == 404:
            return
        response = get(url)

    if response.status_code != 200:
        if response.status_code != 404:
            print(f"Error at id: {newsletter_id}")
            print(response.status_code)
            print(response.reason)
            print(response.text)
        return

    data = BeautifulSoup(response.text, features="lxml")
    newsletter_url, name = parse_name(data)
    title = parse_title(data)
    date = parse_date(data)
    data.select_one(SELECTORS["newsletter_name"])
    return title, name, newsletter_url, newsletter_id, date


def process(newsletters_range: Tuple[int, int], blacklist=None) -> pd.DataFrame:
    """
    Processes every newsletter in the given range, and stores it to a dataframe.
    :param newsletters_range: A tuple representing the range of newsletter to index
    :param blacklist: list of values to ignore.
    :return: dataframe containing the newsletter data
    """
    # more effective might be to first query a batch of newsletters, then process them while waiting on the result for the next batch.
    data = Parallel(backend="threading", n_jobs=128)(delayed(get_newsletter_info)(i) for i in range(newsletters_range[0], newsletters_range[1]) if i not in blacklist)
    df = pd.DataFrame((x for x in data if x is not None), columns=DF_COLUMNS)
    df.set_index(df.id, inplace=True)
    df.drop(columns='id', inplace=True)
    return df


def get_newsletters(newsletters_range: Tuple[int, int], fetch_new_letters: bool = False) -> pd.DataFrame:
    """
    process with a disk cache.
    :param newsletters_range: A tuple representing the range of newsletter to index
    :return: dataframe containing the newsletter data
    """
    if isfile(STORAGE_LOCATION):
        df = pd.read_parquet(STORAGE_LOCATION)
        index = df.index
    else:
        df = None
        index = None

    if not fetch_new_letters:
        return df

    new_df = process(newsletters_range, index)

    if len(new_df) and df is not None:
        # merge dataframes and save to disk
        df = df.append(new_df)
        df.sort_index(inplace=True)
        df.drop_duplicates("title", keep="last", inplace=True)
        df.to_parquet(STORAGE_LOCATION)

    return df
