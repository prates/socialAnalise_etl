import argparse
import os

import pandas as pd

from etl.utils.iotools import IOTools
from etl.task.task_base import  TaskBase


class Extract(TaskBase):


    def clean_df(self, df):
        remove_header = [ 'profile_background_color', 'profile_background_image_url', 'profile_background_image_url_https',
                          'profile_background_tile', 'profile_banner_url', 'profile_image_url', 'profile_image_url_https',
                          'profile_link_color', 'profile_sidebar_border_color', 'profile_sidebar_fill_color',
                          'profile_text_color', 'profile_use_background_image', 'default_profile_image', 'entities',
                          'possibly_sensitive', 'user', 'utc_offset', 'id_str', 'in_reply_to_screen_name',
                          'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id',
                          'in_reply_to_user_id_str', 'is_quote_status', 'time_zone', 'translator_type', 'country',
                          'country_code', 'source' ]
        df.drop(remove_header, inplace=True, axis=1)
        return df

    def clean_source(self, df):
        df['clean_source'] = df['source'].apply(lambda x: x.split('>')[1].split('<')[0])
        return df


    def main(self, input_dir, output_dir):
        for file in os.listdir(input_dir):
            input_file = os.path.join(input_dir, file)
            output_file = os.path.join(output_file, file)
            df = self.read_data(input_file)
            df = self.clean_source(df)
            df = self.clean_df(df)
            self.write_csv(output_file, sep=',')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract information of Tweets')
    parser.add_argument('--input_dir', '-i', required=True)
    parser.add_argument('--output_dir', '-o', required=True)
    args = parser.parse_args()
    e = Extract()
    e.main(args.input_dir, args.output_dir)