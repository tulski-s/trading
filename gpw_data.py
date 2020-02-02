# built in
import csv
import os
import time

# 3rd party
import pandas as pd
import numpy as np

# custom
from price_collector import PriceCollector


class GPWData():
    def __init__(self, pricing_data_path='./pricing_data'):
        self.pricing_data_path = pricing_data_path
        self.collector = PriceCollector()
        self.column_names = ['date', 'open', 'high', 'low', 'close', 'volume']
        self.indicies_stocks = {
            'WIG20': ['ALIOR', 'ASSECOPOL', 'SANPL', 'CCC', 'CYFRPLSAT', 'CDPROJEKT', 'ENERGA',
                      'PLAY', 'KGHM', 'LOTOS', 'LPP', 'MBANK', 'ORANGEPL', 'PEKAO',
                      'PGE', 'PGNIG', 'PKNORLEN', 'PKOBP', 'PZU', 'TAURONPE'],
            'mWIG40': ['11BIT', 'AMICA', 'AMREST', 'JSW', 'BENEFIT', 'BOGDANKA',
                       'BORYSZEW', 'BUDIMEX', 'CIECH', 'CIGAMES', 'COMARCH', 'ECHO',
                       'ENEA', 'DINOPL', 'FAMUR', 'FORTE', 'GETIN', 'GPW', 'GRUPAAZOTY',
                       'GTC', 'HANDLOWY', 'INGBSK', 'INTERCARS', 'KERNEL', 'KETY', 
                       'KRUK', 'LCCORP', 'LIVECHAT', 'MABION', 'MILLENNIUM', 'ORBIS', 
                       'PKPCARGO', 'PLAYWAY', 'POLIMEXMS', 'STALPROD', 'TRAKCJA', 
                       'VISTULA', 'WAWEL', 'WIRTUALNA'
            ]
        }

    def download_data_to_csv(self, symbols=None):
        """
        Collects historical data and outputs csv file in *pricing_data_path*. Currently it takes
        full history until execution and its overwriting files.
        """
        for symbol in symbols:
            print('Downloading {}'.format(symbol))
            pricing_data = self.collector.get_historical_data(symbol)
            with open(self._output_path(symbol), 'w') as fh:
                writer = csv.writer(fh)
                writer.writerow(self.column_names)
                for date, prices in pricing_data.items():
                    writer.writerow([date] + prices)
            time.sleep(0.5)

    def load(self, symbols=None, etfs=None, index=None, df=True, from_csv=True):
        """
        Returns pricing data for *symbols*|*etfs*|*index* (only one should be provided). If *symbols*, then it may be 
        single symbol as a string or  iterable with symbols. *etfs* should be bool (true|false) as it will return data 
        for all posiible ETFs. *index* should be string with index symbol, for exampl 'WIG20'. 

        If *from_csv* is True it reads data from csv, else it gets it from the web. Currently it takes
        full history until execution.

        Output is pandas DataFrame or list of lists with ['date','open','high','low','close','volume']
        if only 1 symbol is provided. If more symbols requested then the output will be a dictionary with 
        symbol as a key and data with the same format as the one for single symbol.
        """
        symbols = self._gather_symbols(symbols, etfs, index)
        pricing_data = {}
        for symbol in symbols:
            if from_csv:
                with open(self._output_path(symbol), 'r') as fh:
                    reader = csv.reader(fh)
                    next(reader)  # skip the header
                    data = [
                        [row[0], float(row[1]), float(row[2]), float(row[3]), float(row[4]), int(row[5])]
                        for row in reader
                    ]
            else:
                data_dict = self.collector.get_historical_data(symbol)
                data = [[date] + prices for date, prices in data_dict.items()]
            # load to DataFrame to fill missing data-points
            date_col = self.column_names[0]
            data = pd.DataFrame(data, columns=self.column_names)
            data.replace(to_replace=0, value=np.nan, inplace=True)
            data.fillna(method='ffill', inplace=True)
            # (back) to desired output format
            if df:
                data.name=symbol
                data.set_index(pd.DatetimeIndex(data[date_col]), inplace=True)
                data.drop(date_col, axis=1, inplace=True)
            else:
                data = data.values.tolist()
            pricing_data[symbol] = data
        if len(symbols) == 1:
            return pricing_data[symbol]
        else:
            return pricing_data

    def detrend(self, data):
        """
        De-trends data (makes average daily price change is equal to zero). 
        Function returns df or list (depends on input type) with new adjusted prices.
        If list, order is as follow: 
        ['date','open','high','low','close','volume','adj_open','adj_high','adj_low','adj_close']
        """
        if isinstance(data, pd.core.frame.DataFrame):
            df = data.copy()
            for column in self.column_names:
                if column in ('date', 'volume'):
                    continue
                # get average daily change
                df.loc[:, f'{column}_change'] = df[column] - df[column].shift(1)
                avg_change = df[f'{column}_change'].mean()
                df.loc[:, f'{column}_change_adj'] = df[f'{column}_change'] - avg_change
                # create new adjusted prices timeseries
                df.fillna(value={f'{column}_change_adj':0}, inplace=True)
                df.loc[:, f'{column}_adj_ch_cumsum'] = df[f'{column}_change_adj'].cumsum()
                df.loc[:, f'adj_{column}'] = df.iloc[0][column] + df[f'{column}_adj_ch_cumsum']
                # clean helper columns
                df.drop(f'{column}_change', axis=1, inplace=True)
                df.drop(f'{column}_change_adj', axis=1, inplace=True)
                df.drop(f'{column}_adj_ch_cumsum', axis=1, inplace=True)
            return df
        elif isinstance(data, list):
            # where col refers to 'open','high','low','close' columns
            new_cols = {}
            for col in range(1,5):
                col_daily_changes = [data[i+1][col] - data[i][col] for i in range(len(data)-1)]
                avg_change = sum(col_daily_changes)/len(col_daily_changes)
                adj_daily_changes = [x-avg_change for x in col_daily_changes]
                new_prices = [data[0][col]]
                for i in range(len(data)-1):
                    new_prices.append(new_prices[i] + adj_daily_changes[i])
                new_cols[col] = new_prices
            new_data = []
            for idx, row in enumerate(data):
                adj_prices = [new_cols[col][idx] for col in range(1,5)]
                new_data.append(row + adj_prices)
            return new_data

    def split_into_subsets(self, pricing_data, ratio, df=True):
        """
        Returns 2 dictionaries - test and validation. Both are in form of {symbol<string>: data<df|dict>}
        *pricing_data* is dict in the form of:
            {'symbol_key': output princing data from load method (df or dictionary)}
        *ratio* defines what portion of data will be in first sample
        """
        # find max date for test set. that will be the base for the split.
        dates = set()
        if df == True:
            for _df in pricing_data.values():
                dates |= set(_df.index.tolist())
        else:
            for vals in pricing_data.values():
                dates |= set([x[0] for x in vals])
        ordered_dates = sorted(list(dates))
        # use ratio to define the split
        max_test_date = ordered_dates[int(len(ordered_dates)*ratio)-1]
        # iterate over signals and cut/move them into test validation collections
        test_set, validation_set = {}, {}
        if df == True:
            for sym, _df in pricing_data.items():
                mask = (_df.index <= max_test_date)
                test_set[sym] = _df[mask]
                validation_set[sym] = _df[~mask]
        else:
            for sym, vals in pricing_data.items():
                test_vals, validation_vals = [], []
                for r in vals:
                    if r[0] <= max_test_date:
                        test_vals.append(r)
                    else:
                        validation_vals.append(r)
                test_set[sym] = test_vals
                validation_set[sym] = validation_vals
        return test_set, validation_set

    def _gather_symbols(self, symbols, etfs, index):
        # if all provided thorw an exception
        is_not_none = [x[0] for x in zip(('symbols','etfs','index'), (symbols, etfs, index)) if x[1] is not None]
        if len(is_not_none) > 1:
            raise TypeError('Should pass only from symbols, etfs, index arguments. Passed: {}'.format(is_not_none))
        elif len(is_not_none) == 0:
            raise TypeError('Argument missing. Pass symbols, etfs or index.')
        type_ = is_not_none[0]
        if type_ == 'symbols':
            if isinstance(symbols, str):
                return [symbols]
            return symbols
        elif type_ == 'etfs':
            return list(self.collector.get_etfs_symbols().keys())
        elif type_ == 'index':
            return self.indicies_stocks[index]

    def _output_path(self, symbol):
        return os.path.join(self.pricing_data_path, '{}_pricing.csv'.format(symbol))


def main():
    pass


if __name__ == '__main__':
    main()


# TODO(stulski) - change prints to proper logging