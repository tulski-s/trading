"""
HMM predictor strategy
"""

# built-in
import itertools
import os
import random
import sys

sys.path.append("/Users/slaw/osobiste/trading")

# 3rd party
import pandas as pd
import numpy as np
from scipy.spatial.distance import (
    euclidean
)
from hmmlearn.hmm import (
    GaussianHMM
)

import sklearn.mixture as mix

from sklearn.model_selection import (
    train_test_split
)
import matplotlib.pyplot as plt

# custom
from gpw_data import GPWData
from strategies.helpers import (
    on_balance_volume_indicator
)


class HmmPredictor():
    def __init__(self, df, symbol_name=None, lookback=12):
        test_split_size = int(df.shape[0]*0.3)
        train_data, test_data = train_test_split(
            df, test_size=test_split_size, shuffle=False
        )
        # train model. n_components is finger in the air for now
        self.hmm = GaussianHMM(n_components=12)
        train_features = self._prepare_features(train_data)
        self.hmm.fit(train_features)
        # creat initial possibilities for prediction
        init_ranges = self._get_init_ranges(train_features)
        init_possibilities, init_dists = self._create_possibile_outcomes(init_ranges)
        # assign attributes
        self.symbol_name = symbol_name
        self.df = df
        self.lookback = lookback
        self.train_data = train_data
        self.test_data = test_data
        self.init_possibilities = init_possibilities
        self.init_dists = init_dists

    def predict_day_close(self, ds):
        # prepare features
        ds_idx = self.df.index.get_loc(ds)
        # ds_df does NOT include ds
        ds_df = self.df[ds_idx-self.lookback:ds_idx]
        # from ds, only open price is taken. close will be predicted
        current_open = self.df.iloc[ds_idx]['open']
        previous_days_features = self._prepare_features(ds_df)
        co_diff = current_open - ds_df.iloc[-1]['close']

        # iteratively look for most possible outcome
        max_iter = 10
        posibilities = self.init_possibilities
        distances = self.init_dists
        old_score = -9999
        iter_no = 0
        while iter_no < max_iter:
            # find best score
            outcome_scores = []
            for idx, p in enumerate(posibilities):
                prediction_day_features = np.append(p, co_diff)
                all_features = np.row_stack(
                    (previous_days_features, prediction_day_features)
                )

                outcome_scores.append(
                    (self.hmm.score(all_features), idx)
                )
            best_score, best_idx = max(outcome_scores)
            most_probable_outcome = posibilities[best_idx] 
            # check convergence
            if euclidean(old_score, best_score) < 0.1:
                break
            # create new ranges
            ranges = []
            for f_idx in range(len(p)):
                ranges.append((
                    most_probable_outcome[f_idx] - distances[f_idx],
                    most_probable_outcome[f_idx] + distances[f_idx]
                ))
            # increment and swap values
            posibilities, distances = self._create_possibile_outcomes(ranges)
            iter_no += 1
            old_score = best_score
        # assumes that 1st feature is frac_change -> (close - open) / open
        new_close = current_open * (1 + most_probable_outcome[0])
        return new_close

    def predcit_for_test_set(self, graph=True, results_path=False):
        days = []
        actual_prices = []
        predicted_prices = []
        actual_open_prices = []
        for day in self.test_data.index:
            ds = str(day)[:10]
            print(f'Processing: {ds}')
            actual_open = self.test_data.iloc[self.test_data.index.get_loc(ds)]['open']
            actual_close = self.test_data.iloc[self.test_data.index.get_loc(ds)]['close']
            predicted_close = self.predict_day_close(ds)

            days.append(ds)
            actual_prices.append(actual_close)
            predicted_prices.append(predicted_close)
            actual_open_prices.append(actual_open)

        if graph == True:
            fig = plt.figure()
            axes = fig.add_subplot(111)
            axes.plot(days, actual_prices, 'bo-', label="actual")
            axes.plot(days, predicted_prices, 'r+-', label="predicted")
            axes.plot(days, actual_open_prices, 'g-', label="actual_open")
            axes.set_title(f'{self.symbol_name}')
            fig.autofmt_xdate()
            plt.legend()
            plt.show()

        if results_path != False:
            new_df = self.test_data.copy()

            print('df shape: ', new_df.shape)
            print('predicted_prices len: ', len(predicted_prices))

            new_df[:, 'predicted_close'] = predicted_prices
            full_path = os.path.join(results_path, f'{self.symbol_name}_predicted.csv')
            new_df.to_csv(full_path)


    def _prepare_features(self, df):
        df = df.copy()
        
        df.loc[:, 'frac_change'] = (df['close'] - df['open']) / df['open']
        df.loc[:, 'range'] = abs(df['high'] - df['low'])
        
        df = on_balance_volume_indicator(df)
        df.loc[:, 'obv_roc'] = df['obv'].pct_change()
        df.loc[:, 'obv_roc_clip'] = df['obv_roc'].clip(lower=-1, upper=1)
        
        df.loc[:, 'prev_close'] = df['close'].shift(1)
        df.loc[:, 'co_diff'] = df['open'] - df['prev_close']
        
        # clean all helpers columns and replaces NaNs
        for _col in ('obv', 'obv_roc', 'prev_close'):
            df.drop(_col, axis=1, inplace=True)
        df.fillna(method='bfill', inplace=True)
        
        frac_change = np.array(df['frac_change'])
        price_range = np.array(df['range'])
        obv_roc = np.array(df['obv_roc_clip'])
        co_diff = np.array(df['co_diff'])

        """
        NOTE: order of features matter:
            - frac_change should be 1st
            - co_diff should be last
        """
        return np.column_stack((frac_change, price_range, obv_roc, co_diff))
        #return np.column_stack((frac_change, price_range, co_diff))

    def _get_init_ranges(self, features):
        """
        note: this assumes that co_diff is the last feature
        """
        f_ranges = []
        # skip co_diff feature as this will be known
        for i in range(features.shape[1]-1):
            arr = features[:,i]
            u = np.std(arr)
            range_start = min(arr) - (3*u)
            range_end = max(arr) + (3*u)
            f_ranges.append((range_start, range_end))
        return f_ranges

    def _create_possibile_outcomes(self, ranges, steps=5):
        f_ranges = []
        dists = []
        for i in range(len(ranges)):
            range_start, range_end = ranges[i]
            # distance between each points
            dists.append((abs(range_start) + abs(range_end)) / (steps-1))
            f_ranges.append(np.linspace(range_start, range_end, steps))
        return np.array(list(itertools.product(*f_ranges))), dists



class HmmRegimePredictor():
    def __init__(self, df, symbol_name=None):
        test_split_size = int(df.shape[0]*0.3)
        train_data, test_data = train_test_split(
            df, test_size=test_split_size, shuffle=False
        )
        # train model. n_components is finger in the air for now
        self.hmm = mix.GaussianMixture(
            n_components=3, 
            covariance_type="full", 
            n_init=100, 
            random_state=7
        )
        train_features = self._prepare_features(train_data)
        self.hmm.fit(train_features)
        # predict regimes
        test_features = self._prepare_features(test_data)
        self.regimes = self.hmm.predict(test_features)
        # assign attributes
        self.symbol_name = symbol_name
        self.df = df
        self.train_data = train_data
        self.test_data = test_data

    def plot_regimes(self):
        fig = plt.figure()
        axes = fig.add_subplot(111)
        days = self.test_data.index.tolist()
        close = self.test_data['close'].tolist()
        axes.scatter(days, close, c=self.regimes)
        axes.set_title(f'{self.symbol_name}')
        fig.autofmt_xdate()
        plt.legend()
        plt.show()
    
    def _prepare_features(self, df):
        df = df.copy()
        
        df.loc[:, 'frac_change'] = (df['close'] - df['open']) / df['open']
        df.loc[:, 'range'] = abs(df['high'] - df['low'])
        
        df = on_balance_volume_indicator(df)
        df.loc[:, 'obv_roc'] = df['obv'].pct_change()
        df.loc[:, 'obv_roc_clip'] = df['obv_roc'].clip(lower=-1, upper=1)
        
        df.loc[:, 'prev_close'] = df['close'].shift(1)
        df.loc[:, 'co_diff'] = df['open'] - df['prev_close']
        
        # clean all helpers columns and replaces NaNs
        for _col in ('obv', 'obv_roc', 'prev_close'):
            df.drop(_col, axis=1, inplace=True)
        df.fillna(method='bfill', inplace=True)
        
        frac_change = np.array(df['frac_change'])
        price_range = np.array(df['range'])
        obv_roc = np.array(df['obv_roc_clip'])
        co_diff = np.array(df['co_diff'])

        """
        NOTE: order of features matter:
            - frac_change should be 1st
            - co_diff should be last
        """
        return np.column_stack((frac_change, price_range, obv_roc, co_diff))


def main():
    gpw = GPWData(pricing_data_path='/Users/slaw/osobiste/trading/pricing_data')
    symbols = gpw.indicies_stocks['mWIG40']
    data = gpw.load(symbols=symbols)

    sym = 'WIRTUALNA'
    df = data[sym]

    # print('Predict close price for each day')
    # hmm = HmmPredictor(df, symbol_name=sym, lookback=10)
    # hmm.predcit_for_test_set(
    #     graph=True,
    # )

    print('Predict regimes')
    hmm = HmmRegimePredictor(df, symbol_name=sym)
    hmm.plot_regimes()



if __name__ == '__main__':
    main()