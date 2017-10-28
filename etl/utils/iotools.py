import json
import pandas as pd

class IOTools():

    def read_json(self, file_name):
        lines = []
        with open(file_name, 'r') as f:
            lines = f.readlines()
        data = [json.loads(line) for line in lines]
        data = pd.DataFrame(data)
        user = data['user'].apply(pd.Series)
        user.rename(columns={'created_at':'user_created'}, inplace=True)
        places = data['place'].apply(pd.Series)
        entity = data['entities'].apply(pd.Series)
        data.drop(['user', 'entities', 'place'], axis=1)
        return pd.concat([data, user, entity, places], axis=1)

    def write_csv(self, file_name, df, sep, index=False):
        df.to_csv(file_name, sep=sep, index=index)

    def read_csv(self, file_name, sep=',', header=True):
        return pd.read_csv(file_name, sep=sep, header=header)

    def read_file_profission_list(self, file_name, encoding='latin-1'):
        profission_list = []
        with open(file_name, 'r', encoding=encoding) as f:
            lines = f.readlines()
            for line in lines:
                if len(line) > 2:
                    profission_list.append(line.split(';')[1].lower())
        return profission_list

        
if __name__ == '__main__':
    import sys
    io = IOTools()
    data = io.read_json(sys.argv[1])
    io.write_csv('teste.csv', df=data, sep=';')
    #print(data.head())
    #print(data.columns)
