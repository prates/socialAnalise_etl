from argparse import ArgumentParser
import os
import pickle

from etl.task.task_base import TaskBase
from etl.utils.wordprocessor import WordProcessor

class TagSentimnets(TaskBase):

    def load_naive_bayes(self, file_name):
        cl = None
        with open(file_name, 'rb') as f:
            cl = pickle.load(file=f)
        return cl

    def clean_sentences(self, text):
        wp = WordProcessor()
        text = wp.valid_words(text)
        text = wp.remove_stop_words(text)
        return text

    def process(self, input_dir, output_dir, classification_file):
        cl = self.load_naive_bayes(classification_file)
        for file in os.listdir(input_dir):
            input_file = os.path.join(input_dir, file)
            output_file = os.path.join(output_dir, file)
            df = self.read_csv(input_file, sep=',')
            wp = WordProcessor()
            df['text'] = df['text'].apply(wp.remove_stop_words)
            df['sentiment'] = df['text'].apply(cl.classify)
            self.write_csv(file_name=output_file, df=df, sep=',')


if __name__ == '__main__':
    parser = ArgumentParser(description='')
    parser.add_argument('--input_folder', '-i', help='input folder', required=True)
    parser.add_argument('--output_folder', '-o', help='output folder', required=True)
    parser.add_argument('--classification', '-cl', help= 'object of classificator', required=True)
    args = parser.parse_args()

    ts = TagSentimnets()
    ts.process(input_dir=args.input_folder, output_dir=args.output_folder, classification_file=args.classification)

