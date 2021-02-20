# built in
import csv
import datetime
import os
import statistics
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
            pricing_data = self._download_with_retry(symbol, from_date)
            data = self._pricing_data_2_rows(pricing_data)
            mode = 'w' if append_csv == False else 'a'
            with open(self._output_path(symbol), 'w') as fh:
                writer = csv.writer(fh)
                writer.writerow(self.column_names)
                for row in data:
                    writer.writerow(row)
            time.sleep(1)

    def incremental_download_to_csv(self, symbols=None):
        """
        Similar to download_data_to_csv, but looks for last available date. Download data
        from that date (including last available) and write/over-write new data.
        """
        print('Start incremental load')
        for symbol in symbols:
            print(f'Downloading {symbol}')
            org_data = []
            with open(self._output_path(symbol), 'r') as fh:
                reader = csv.reader(fh)
                for row in reader:
                    org_data.append(tuple(row))
            last_aval_date = org_data[-1][0]
            pricing_data = self._download_with_retry(symbol, last_aval_date)
            new_data = self._pricing_data_2_rows(pricing_data)
            inc_data = org_data[:-1] + new_data
            with open(self._output_path(symbol), 'w') as fh:
                writer = csv.writer(fh)
                for row in inc_data:
                    writer.writerow(row)
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

    def _download_with_retry(self, symbol, from_date):
        try:
            pricing_data = self._get_stock_historical_data(symbol, from_date=from_date)
        except ConnectionError:
            print('Some connection issue. Waiting 60s and retrying')
            time.sleep(60)
            pricing_data = self._get_stock_historical_data(symbol, from_date=from_date)
        return pricing_data

    def _pricing_data_2_rows(self, pricing_data):
        dates = [d.date().strftime('%Y-%m-%d') for d in pricing_data.index.to_list()]
        data_arrs = []
        for col in self.column_names[1:]:
            data_arrs.append(pricing_data[col.capitalize()].to_list())
        data = list(zip(*data_arrs))
        rows = []
        for idx, d in enumerate(dates):
            rows.append(
                (d,) + data[idx]
            )
        return rows


class LSECleaner():
    def __init__(self):
        self._path = '/Users/slaw/osobiste/trading/pricing_data'

    def truncate_missing_data(self):
        symbols_to_truncate = {         
            'SKG': '2016-02-22',
            'GVC': '2005-01-07',
            'DCC': '2012-03-22',
            'RR': '2018-10-04',
            'RBS': '2012-06-01',
        }
        oct_2nd = [
            'III', 'AZN', 'BAES', 'BDEV', 'BT', 
            'BRBY', 'LSE', 'NXT', 'PRU', 'RDSa',
            'STAN', 'TSCO', 'WPP',
        ]
        for s in oct_2nd:
            symbols_to_truncate[s] = '2009-10-02'
        all_symbols = [el['symbol'] for el in ftse_100]
        for sym in all_symbols:
            # going over all symbol just for convinience. I will later drop old files and replace
            # them with new ones. so want to have every symbol processed same way
            print(f'\t {sym}')
            input_path = os.path.join(self._path, f'new_{sym}_pricing.csv')
            output_path = os.path.join(self._path, f'new_truc_{sym}_pricing.csv')
            with open(input_path, 'r') as fh_r:
                reader = csv.reader(fh_r)
                with open(output_path, 'w') as fh_w:
                    writer = csv.writer(fh_w)
                    writer.writerow(next(reader))
                    if sym not in symbols_to_truncate:
                        # just copy each row unchanged
                        for row in reader:
                            writer.writerow(row)
                    else:
                        for row in reader:
                            if row[0] < symbols_to_truncate[sym]:
                                continue
                            else:
                                writer.writerow(row)
        print('Done truncatiing all symbols')

    def remove_spikes(self):
        symbols_to_clean = [el['symbol'] for el in ftse_100]

        no_spikes = {}  # to track if not filtering too much
        lookback = 7  # used for getting median value to repalce outlier
        for sym in symbols_to_clean:
            print(f'\t {sym}')
            no_spikes[sym] = 0
            input_path = os.path.join(self._path, '{}_pricing.csv'.format(sym))
            with open(input_path, 'r') as fh:
                reader = csv.reader(fh)
                next(reader)
                csv_data = [(idx, row) for idx, row in enumerate(reader)]
            csv_len = len(csv_data)
            # offset helps to detect spikes that persiists up to x days. e.g. 1d, 2d spikes
            offset = 2  # up to 2d spikes
            processed_rows = [row for idx, row in csv_data[0:offset]]
            for idx, row in csv_data[offset:]:
                if idx >= csv_len-offset:
                    processed_rows.append(row)
                    continue
                prev_close = float(csv_data[idx-offset][1][4])
                cur_close = float(row[4])
                next_close = float(csv_data[idx+offset][1][4])
                prev_pct_diff = self._perc_change(prev_close, cur_close)
                next_pct_diff = self._perc_change(cur_close, next_close)
                # percentage threshold defining spike. e.g 40% daily up/down
                threshold = 40
                spike_up = (prev_pct_diff >= threshold) and (next_pct_diff <= -threshold)
                spike_down = (prev_pct_diff <= -threshold) and (next_pct_diff >= threshold)
                if spike_up or spike_down:
                    no_spikes[sym] += 1
                    print(row[0], prev_close, cur_close, next_close)
                    start = idx-lookback if idx-lookback > 0 else 0
                    end = idx
                    new_row = []
                    # 1:open 2:high, 3:low, 4:close, 5:volume
                    for label in range(1,6):
                        new_row.append(
                            statistics.median([row[label] for idx, row in csv_data[start:end]])
                        )
                    processed_rows.append([row[0]] + new_row)
                else:
                    processed_rows.append(row)
            print(f'csv_len: {csv_len} ,  processed_rows: {len(processed_rows)} ')
            assert(csv_len == len(processed_rows))
            output_path = os.path.join(self._path, 'new_{}_pricing.csv'.format(sym))
            with open(output_path, 'w') as fh:
                writer = csv.writer(fh)
                writer.writerow(['date', 'open', 'high', 'low', 'close', 'volume'])
                for row in processed_rows:
                    writer.writerow(row)
        print('Done removing spikes from all symbols')
        print(no_spikes)

    def _perc_change(self, v1, v2):
        return ((v2-v1)/abs(v1))*100


def test():
    lse = LSEData()
    # data = lse.load(
    #     symbols=['WTB', 'IHG'],
    #     from_csv=True,
    # )
    lse.incremental_download_to_csv(
        symbols=lse.indicies_stocks['FTSE100']
    )
    # lse.download_data_to_csv(symbols=['III', ])


def clean_data():
    c = LSECleaner()
    # c.remove_spikes()
    # c.truncate_missing_data()


if __name__ == '__main__':
    test()
    # clean_data()
