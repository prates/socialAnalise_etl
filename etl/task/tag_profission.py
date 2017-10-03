from argparse import ArgumentParser
import os
import re


from etl.task.task_base import TaskBase

class TagProfession(TaskBase):

    valid_word = re.compile('[a-z]+')

    def load_profisions_list(self, file_name):
        profission_list = self.io.read_file_profission_list(file_name)
        return profission_list

    def tokenize(self, words):
        word_prop = []
        for word in words.split(' '):
            if len(word) > 3:
                if re.match(word):
                    word_prop.append(word)
        return word_prop

    def clean_profission(self, profission_list):
        profission_list_clean = []
        for profission in profission_list:
            profission_list_clean.append(profission.split(' ')[0].lower())
        return set(profission_list_clean)

    def match_profission(self, description):
        #TODO pensar em um jeito de agrupar profissoes com mais de 1 palavra
        words = self.tokenize(description.lower())
        profission_prop = 'unknown'
        for word in words:
            for profission in self.profision_list:
                if profission == word:
                    profission_prop = word
                    break
        return profission

    def process_description(self, user_df):
        user_df['profission'] = user_df['description'].apply(self.match_profission)
        del(user_df['description'])
        return user_df

    def main(self, output_folder, input_folder, profission_file):
        for file in os.listdir(input_folder):
            file_input = os.path.join(input_folder, file)
            file_output = os.path.join(output_folder, file)
            user_df = self.read_csv(file_input)
            profision_list = self.load_profisions_list(profission_file)
            self.profission_list = self.clean_profission(self.profision_list)
            df = self.process_description(user_df)
            self.write_csv(file_output, df, sep=',')

if __name__  ==  '__main__':
    parser = ArgumentParser(description='Tag most prob profiission of user')
    parser.add_argument('--input_dir', '-i', help='input folder', required=True)
    parser.add_argument('--output_dir', '-i', help='output_folder', required=True)
    parser.add_argument('--profission_file', '-pf', help='file with CBO list', required=True)
    args = parser.parse_args()
    t = TagProfession()
