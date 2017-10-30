from datetime import datetime
import os
from argparse import ArgumentParser


from elasticsearch import Elasticsearch
from elasticsearch import helpers

from etl.task.task_base import TaskBase

class Indexer(TaskBase):

    def load_es_connection(self, esHost):
        print("Loading ES connection...")
        es = Elasticsearch(
            host=esHost,
            port=443,
            use_ssl=True,
            verify_certs=False,
        )
        print(es.info())
        return es

    def convert_int(self, x):
        if x == 'n/a':
            x = 0
        else:
            x = int(x)
        return x

    def convert_bool(self, x):
        if x:
            x = "T"
        else:
            x = "F"
        return x

    def index_tweets(self, df, es, index):
        row_list = []
        print(df.dtypes)
        for i in df.index:
            print(i)
            row = df.iloc[i].to_dict()

            if not row['lat'] or not row['lon']:
                continue

            try:
                lat = float(row['lat'])
                lon = float(row['lon'])
            except Exception as ex:
                print(ex)
                continue
            k = ({
                "_index": index,
                "_type": "tweet",
                'ponto': {
                    'lat': lat,
                    'lon': lon
                },
                "created_at": datetime.strptime(row["created_at"], '%Y-%m-%d %H:%M:%S %z'),
                "favorite_count": self.convert_int(row["favorite_count"]),
                "lang": row["lang"],
                "quote_count": self.convert_int(row["quote_count"]),
                "reply_count": self.convert_int(row["reply_count"]),
                "retweet_count": self.convert_int(row["retweet_count"]),
                "retweeted": self.convert_bool(row["retweeted"]),
                "text": row["text"],
                "truncated": self.convert_bool(row["truncated"]),
                "contributors_enabled": self.convert_bool(row["contributors_enabled"]),
                "user_created": datetime.strptime(row["user_created"], '%Y-%m-%d %H:%M:%S %z'),
                "default_profile": self.convert_bool(row["default_profile"]),
                "description": row["description"],
                "favourites_count": self.convert_int(row["favourites_count"]),
                "followers_count": self.convert_int(row["followers_count"]),
                "following": self.convert_int(row["following"]),
                "friends_count": self.convert_int(row["friends_count"]),
                "listed_count": self.convert_int(row["listed_count"]),
                "protected": self.convert_bool(row["protected"]),
                "statuses_count": self.convert_int(row["statuses_count"]),
                "verified": self.convert_bool(row["verified"]),
                "mentions": row["mentions"],
                "hash_tags": row["hash_tags"],
                "clean_source": row["clean_source"],
                "sentiment": row["sentiment"],

            })
            row_list.append(k)
        try:
            helpers.bulk(es, (row_list))
        except Exception as ex:
            print('\n\n\n\n', ex, '\n\n\n\n')

    def process(self, input_dir, index_name, host):
        for file in os.listdir(input_dir):
            input_file = os.path.join(input_dir, file)
            df = self.read_csv(file_name=input_file, sep=',')
            es = self.load_es_connection(host)
            self.index_tweets(df=df, es=es, index=index_name)


if __name__ == '__main__':
    parser = ArgumentParser(description='')
    parser.add_argument('--input_folder', '-i', help='input folder', required=True)
    parser.add_argument('--index_name', '-id', help='index name', required=True)
    parser.add_argument('--host_elasticsearch', '-es', help= 'end point elasticsearch', required=True)
    args = parser.parse_args()

    Ind = Indexer()
    Ind.process(input_dir=args.input_folder, index_name=args.index_name, host=args.host_elasticsearch)