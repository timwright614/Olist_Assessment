import os
import pandas as pd


class Olist:
    def get_data(self):
        """
        This function returns a Python dict.
        Its keys should be 'sellers', 'orders', 'order_items' etc...
        Its values should be pandas.DataFrames loaded from csv files
        """
        csv_path = '''/Users/timothywright/code/timwright614/data-challenges/04-Decision-Science/data/csv'''
        file_names = [i for i in os.listdir('''/Users/timothywright/code/timwright614/data-challenges/04-Decision-Science/data/csv''') if '.csv' in i]
        key_names = [i.replace('olist_','').replace('_dataset.csv','').replace('.csv','') for i in file_names]

        data = {}

        for (key,file) in zip (key_names,file_names):

            data[key] = pd.read_csv(os.path.join(csv_path, file))

        return data

    def ping(self):
        """
        You call ping I print pong.
        """
        print("pong")
