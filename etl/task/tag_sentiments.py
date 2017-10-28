from argparse import ArgumentParser
import os
import pickle

from etl.task.task_base import TaskBase
from etl.utils.wordprocessor import WordProcessor

class TagSentimnets(TaskBase):

    def load_naive_bayes(self, file_name):
        cl = None
        with open(file_name, 'wb') as f:
            cl = pickle._load(f)
        return cl

    def clean_sentences(self, text):
        wp = WordProcessor()
        text = wp.valid_words(text)
        text = wp.remove_stop_words(text)
        return text

    def process(self):
        #TODO carregar o modelo e fazer a calssificacao
        pass


if __name__ == '__main__':
    parser = ArgumentParser(description='')
    parser.add_argument('--input_folder', '-i', help='input folder', required=True)
    parser.add_argument('--output_folder', '-o', help='output folder', requided=True)
    parser.add_argument('--classification', '-cl', 'object of classificator', required=True)
    args = parser.parse_args()

