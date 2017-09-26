import unittest

from etl.utils.wordprocessor import WordProcessor

class TestWordProcessor(unittest.TestCase):

    def setUp(self):
        self.word = WordProcessor()

    def test_remove_url(self):
        tweet = "O jogo pode ser acompanhado em https://xpto.com/jogo."
        expected = "O jogo pode ser acompanhado em ."
        clean_str = self.word.remove_url(tweet)
        self.assertEqual(clean_str, expected)

    def test_remove_stop_words(self):
        tweet = "a reuniao ocorreu de maneira proveitosa"
        expected = "reuniao ocorreu maneira proveitosa"
        clean_str = self.word.remove_stop_words(tweet)
        self.assertEqual(clean_str, clean_str)

if __name__ == '__main__':

    unittest.main()
