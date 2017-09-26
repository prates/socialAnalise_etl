import pandas as pd

from etl.utils.iotools import IOTools

class TaskBase():

    def read_data(self, file_name):
        io = IOTools()
        data = io.read_json(file_name)
        return data.fillna('n/a', axis=1)


    def write_csv(self, file_name, df, sep):
        io = IOTools()
        io.write_csv(file_name=file_name, df=df, sep=sep)