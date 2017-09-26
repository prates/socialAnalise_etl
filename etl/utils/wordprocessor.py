import re

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

class WordProcessor():

    url = re.compile('https*://.*( |.|,|!)+')

    def remove_url(self, text):
        return ''.join(self.url.split(text))

    def remove_stop_words(self, text):
        stop_words = stopwords.words('portuguese')
        text = text.lower()
        tokens = word_tokenize(text, language='portuguese')
        return ' '.join([ word for word in tokens if not word  in stop_words])

