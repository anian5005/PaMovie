import string
from difflib import SequenceMatcher

# s1 = 'top gun'
# s2 = 'top gun:'
# s3 = '今天天氣真好'


def match_preprocessing(movie_1, movie_2):


    def preprocess(str):
        text = str.lower()
        for char in string.punctuation:
            text = text.replace(char, " ")
            return text

    movie_1 = preprocess(movie_1)
    movie_2 = preprocess(movie_2)
    ratio = SequenceMatcher(lambda x: x in " ", movie_1, movie_2).ratio()
    return ratio


# r = match_preprocessing(s1, s2)
# print(r)
