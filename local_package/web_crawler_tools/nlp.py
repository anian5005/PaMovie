# Standard library imports
import string

# Third party imports
from difflib import SequenceMatcher


def preprocess(str):
    text = str.lower()
    for char in string.punctuation:
        text = text.replace(char, " ")
        return text


def match_preprocessing(movie_1, movie_2):
    movie_1 = preprocess(movie_1)
    movie_2 = preprocess(movie_2)
    ratio = SequenceMatcher(lambda x: x in " ", movie_1, movie_2).ratio()
    return ratio

