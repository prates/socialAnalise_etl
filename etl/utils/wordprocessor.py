import re

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

class WordProcessor():

    url = re.compile('https*://.*( |.|,|!)+')

    users = re.compile('@([a-z]|[A-Z]|[0-9]|_){1,15}')

    hash_tag = re.compile('#([a-z]|[A-Z]|[0-9]|_)*')

    word = re.compile('[a-z]+')

    def remove_url(self, text):
        return ''.join(self.url.split(text))

    def remove_user(self, text):
        return ''.join(self.users.split(text))

    def remove_hash_tag(self, text):
        return ''.join(self.hash_tag.split(text))

    def remove_stop_words(self, text):
        stop_words = stopwords.words('portuguese')
        text = text.lower()
        tokens = word_tokenize(text, language='portuguese')
        return ' '.join([ word for word in tokens if not word  in stop_words])

    def valid_words(self, text):
        text = text.lower()
        return ' '.join(self.word.findall(text))