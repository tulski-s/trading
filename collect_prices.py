# built-in
import csv
import os
import time

# 3rd party
import numpy as np
import pandas as pd

# custom
from collector import Collector

class PricingDataManager():
    def __init__(self, pricing_data_path='./pricing_data'):
        self.pricing_data_path = pricing_data_path

        self.wig20_stocks = ['ALIOR','ASSECOPOL','BZWBK','CCC','CYFRPLSAT','ENEA',
                             'ENERGA','EUROCASH','KGHM','LOTOS','LPP','MBANK',
                             'ORANGEPL','PEKAO','PGE','PGNIG','PKNORLEN','PKOBP','PZU','TAURONPE']
        self.c = Collector()

    def collect(self):
        # will collect all historical data until today. it's overwtiting files        
        for symbol in self.wig20_stocks:
            pricing_data = self.c.get_historical_data(symbol)

            with open(self._output_path(symbol), 'w') as output_file:
                writer = csv.writer(output_file)
                writer.writerow(self.c.col_names)
                for entry in pricing_data:
                    writer.writerow(entry)
            print('got ', symbol)
            time.sleep(1)

    def load_all(self):
        # loads all stocks to dict where key is symbol and value is formatted DataFrame
        stocks_data = {}
        for symbol in self.wig20_stocks:
            output_path = self._output_path(symbol)
            if os.path.exists(output_path):
                df = self._load_from_csv(output_path)
                stocks_data[symbol] = df
        return stocks_data

    def load_stock(self, symbol):
        # loads pricing data for given stock symbol to dataframe
        output_path = self._output_path(symbol)
        if os.path.exists(output_path):
            df = self._load_from_csv(output_path)
            return df

    def _output_path(self, symbol):
        return os.path.join(self.pricing_data_path, '{}_pricing.csv'.format(symbol))

    def _load_from_csv(self, path):
        df = pd.read_csv(path)
        # fill missing pirces (0.0) for sessions with previous valid value
        df.replace(to_replace=0, value=np.nan, inplace=True)
        df.fillna(method='ffill', inplace=True)
        self._prepare_df(df)
        return df

    def _prepare_df(self, df):
        df.set_index(pd.DatetimeIndex(df['timestamp']), inplace=True)
        df.drop('timestamp', axis=1, inplace=True)
                
        for col in df.columns:
            if col not in ('name',):
                df[col] = df[col].astype('float64')

def main():
    pm = PricingDataManager()
    #pm.collect()
    # data = pm.load_all()
    data = pm.load_stock('LPP')
    print(data.ix['2010-08-18'])

if __name__ == '__main__':
    main()