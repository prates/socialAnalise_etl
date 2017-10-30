import pandas as pd

from etl.utils.iotools import IOTools


class TaskBase():

    def __init__(self):
        self.io = IOTools()

    def read_data(self, file_name):
        data = self.io.read_json(file_name)
        return data.fillna('n/a', axis=1)


    def write_csv(self, file_name, df, sep):
        self.io.write_csv(file_name=file_name, df=df, sep=sep)


    def read_csv(self, file_name, sep=','):
        return pd.read_csv(file_name, sep=sep)
