# built in
import csv
import datetime
import os
import time

# 3rd party
import dateparser
import investpy
import pandas as pd
import numpy as np


# custom
from ftse_symbols import (
    ftse_100,
) 


class LSEData():
    def __init__(self, pricing_data_path='./pricing_data'):
        self.country = 'united kingdom'
        self.indicies_stocks = {
            'FTSE100': [el['symbol'] for el in ftse_100]
        }
        self.pricing_data_path = pricing_data_path
        self.column_names = ['date', 'open', 'high', 'low', 'close', 'volume']

    def load(self, symbols=None, df=True, from_csv=True):
        """
        Returns pricing data for *symbols*. *symbols* may be single symbol as a string or
        iterable with symbols. 

        If *from_csv* is True it reads data from csv, else it gets it from the web. Currently it takes
        full history until execution.

        Output is pandas DataFrame or list of lists with ['date','open','high','low','close','volume']
        if only 1 symbol is provided. If more symbols requested then the output will be a dictionary with 
        symbol as a key and data with the same format as the one for single symbol.
        """
        if isinstance(symbols, str):
            symbols = [symbols]
        pricing_data = {}
        for symbol in symbols:
            if from_csv:
                data_df = pd.read_csv(self._output_path(symbol))
                date_col = self.column_names[0]
                data_df.set_index(pd.DatetimeIndex(data_df[date_col]), inplace=True)
                data_df.drop(date_col, axis=1, inplace=True)
            else:
                data_df = self._get_stock_historical_data(symbol)
                data_df.drop('Currency', axis=1, inplace=True)
                data_df.rename(columns={c: c.lower() for c in data_df.columns}, inplace=True)
                data_df.index.rename('date', inplace=True)
            data_df.name=symbol
            data_df.replace(to_replace=0, value=np.nan, inplace=True)
            data_df.fillna(method='ffill', inplace=True)
            if df == True:
                pricing_data[symbol] = data_df
            else:
                pricing_data[symbol] = [
                    [datetime.datetime.fromtimestamp(d.item()//1000000000).strftime('%Y-%m-%d')] + val
                    for d,val in list(zip(data_df.index.values, data_df.values.tolist()))
                ]
        if len(symbols) == 1:
            return pricing_data[symbol]
        else:
            return pricing_data

    def download_data_to_csv(self, symbols=None, from_date=None, append_csv=False):
        """
        Collects historical data and outputs csv file in *pricing_data_path*. Currently it takes
        history from 1990 until execution date and its overwriting files.
        """
        for symbol in symbols:
            print('Downloading {}'.format(symbol))
            try:
                pricing_data = self._get_stock_historical_data(symbol, from_date=from_date)
            except ConnectionError:
                print('Some connection issue. Waiting 60s and retrying')
                time.sleep(60)
                pricing_data = self._get_stock_historical_data(symbol, from_date=from_date)
            dates = [d.date().strftime('%Y-%m-%d') for d in pricing_data.index.to_list()]
            data_arrs = []
            for col in self.column_names[1:]:
                data_arrs.append(pricing_data[col.capitalize()].to_list())
            data = list(zip(*data_arrs))
            mode = 'w' if append_csv == False else 'a'
            with open(self._output_path(symbol), 'w') as fh:
                writer = csv.writer(fh)
                writer.writerow(self.column_names)
                for idx, d in enumerate(dates):
                    writer.writerow((d,) + data[idx])
            time.sleep(1)

    def get_all_available_symbols(self):
        """
        Return list of all available stocks for UK in investpy
        """
        stocks_df = investpy.get_stocks(country=self.country)
        return stocks_df['symbol'].to_list()

    def _output_path(self, symbol):
        return os.path.join(self.pricing_data_path, '{}_pricing.csv'.format(symbol))

    def _get_stock_historical_data(self, symbol, from_date=None):
        if from_date != None:
            from_date_obj = dateparser.parse(from_date)
            from_date = from_date_obj.strftime('%d/%m/%Y')
        else:
            from_date = '01/01/1990'
        return investpy.get_stock_historical_data(
            stock=symbol,
            country=self.country,
            from_date=from_date,
            to_date=datetime.date.today().strftime('%d/%m/%Y'),
        )


def test():
    lse = LSEData()
    data = lse.load(
        symbols=['WTB', 'IHG'],
        from_csv=True,
    )


if __name__ == '__main__':
    test()

