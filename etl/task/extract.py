import argparse
import os
from datetime import datetime
import json

import pandas as pd

from etl.task.task_base import  TaskBase
from etl.utils.wordprocessor import WordProcessor

class Extract(TaskBase):



    def clean_df(self, df):
        remove_header = [ 'profile_background_color', 'profile_background_image_url', 'profile_background_image_url_https',
                          'profile_background_tile', 'profile_banner_url', 'profile_image_url', 'profile_image_url_https',
                          'profile_link_color', 'profile_sidebar_border_color', 'profile_sidebar_fill_color',
                          'profile_text_color', 'profile_use_background_image', 'default_profile_image', 'entities',
                          'possibly_sensitive', 'user', 'utc_offset', 'id_str', 'in_reply_to_screen_name',
                          'in_reply_to_status_id', 'in_reply_to_status_id_str', 'in_reply_to_user_id',
                          'in_reply_to_user_id_str', 'is_quote_status', 'time_zone', 'translator_type', 'country',
                          'country_code', 'source', 'extended_entities', 'place', 'bounding_box', 'screen_name',
                          'location', 'name', 'notifications', 'screen_name', 'url', 'media', 'urls',
                          'attributes', 'bounding_box', 'full_name', 'name', 'place_type', 'geo_enabled', 'id',
                          'follow_request_sent', 'filter_level', 'favorited', 'extended_tweet', 'display_text_range',
                           'contributors', 'quoted_status', 'quoted_status', 'quoted_status_id', 'quoted_status_id_str',
                          'coordinates', 'geo', 'user_mentions', 'hashtags']
        df.drop(remove_header, inplace=True, axis=1)
        return df

    def clean_source(self, df):
        df['clean_source'] = df['source'].apply(lambda x: x.split('>')[1].split('<')[0])
        return df

    def filter_geo(self, df):
        df = df[df['geo']!='n/a']
        return df

    def clean_coordinates(self, data):
        if data['type'] == 'Point':
            return data['coordinates']
        return ['n/a', 'n/a']

    def remove_user_mentions(self, df):
        wp = WordProcessor()
        df['text'] = df['text'].apply(wp.remove_user)
        return df

    def get_mentions(self, data):
        mentions = []
        for i in data:
            mentions.append(i['screen_name'])
        return mentions


    def process_mentions(self, df):
        df['mentions'] = df['user_mentions'].apply(self.get_mentions)
        return df

    def format_lat_lon(self, df):
        coord = df['coordinates'].apply(self.clean_coordinates)
        df['lat'] = coord.apply(lambda x: x[1])
        df['lon'] = coord.apply(lambda x: x[0])
        return df

    def convert_date(self, df, fields):
        for i in fields:
            df[i] = df[i].apply(lambda x: datetime.strptime(x, '%a %b %d %H:%M:%S %z %Y'))
        return df

    def get_hashtags(self, data):
        mentions = []
        for i in data:
            mentions.append(i['text'])
        return mentions

    #TODO Calcular o tempo de criacao do usuario

    def process_hashtags(self, df):
        df['hash_tags'] = df['hashtags'].apply(self.get_hashtags)
        return df

    def main(self, input_dir, output_dir):
        for file in os.listdir(input_dir):
            input_file = os.path.join(input_dir, file)
            output_file = os.path.join(output_dir, file)
            df = self.read_data(input_file)
            df = self.filter_geo(df)
            df = self.format_lat_lon(df)
            df = self.remove_user_mentions(df)
            df = self.process_mentions(df)
            df = self.process_hashtags(df)
            df = self.convert_date(df, ['created_at', 'user_created'])

            df = self.clean_source(df)
            df = self.clean_df(df)
            #print(df.columns)
            self.write_csv(output_file, df, sep=',')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract information of Tweets')
    parser.add_argument('--input_dir', '-i', required=True)
    parser.add_argument('--output_dir', '-o', required=True)
    args = parser.parse_args()
    e = Extract()
    e.main(args.input_dir, args.output_dir)