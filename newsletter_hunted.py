from html import escape
from typing import Tuple

import pandas as pd

from preprocessor import get_newsletters, NEWSLETTER_URL

DEFAULT_MAX_RANGE = 32300
LETTERS_SAVE_PATH = "./letters.html"
FETCH_NEW_LETTERS = False


def render_matching_newsletters_to_html(letter_url: str, search_range: Tuple[int, int] = (1, DEFAULT_MAX_RANGE)):
    """
    Obtain, filter, and render the list to an HTML table.
    """
    letters = get_newsletters(search_range, FETCH_NEW_LETTERS)
    if letters is None:
        return
    filtered_letters: pd.DataFrame = letters[letters.newsletter_url == letter_url].copy()
    filtered_letters = create_newsletter_links(filtered_letters)
    filtered_letters.to_html(LETTERS_SAVE_PATH, escape=False)


def create_newsletter_links(newsletter_table: pd.DataFrame):
    """
    Converts the name and title column of the table to contain hyperlinks
    """
    newsletter_table.name = newsletter_table.apply(lambda x: create_link(x.newsletter_url, x["name"]), axis=1)
    newsletter_table.title = newsletter_table.apply(lambda x: create_link(NEWSLETTER_URL.format(x.name), x.title), axis=1)
    newsletter_table = newsletter_table.drop("newsletter_url", axis=1)
    return newsletter_table


def create_link(url, name):
    """
    :return: HTML formatted link `name` to `url`
    """
    return f'<a href="{url}" rel="noopener noreferrer" target="_blank">{escape(name).encode("ascii", "xmlcharrefreplace").decode("utf-8")}</a>'


if __name__ == '__main__':
    # first, we select the newsletter we are interested in
    target_newsletter = "https://newsletterhunt.com/newsletters/money-stuff-by-matt-levine"
    # and save them all to a file.
    render_matching_newsletters_to_html(target_newsletter)
