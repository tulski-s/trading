# build-in

# 3rd party
import numpy as np
import pandas as pd

# custom
from gpw_data import GPWData


def get_strategy_signals(symbol):
    data = GPWData()
    etf_full = data.load(symbols=symbol)

    # strategy params (should come as params but hardcoded here for simplicity)
    time_window=3
    long_threshold=35
    short_threshold=30

    # split data into optimize/validate
    length = len(etf_full.index)
    etf_t = etf_full.iloc[:length//2, :].copy()
    etf_v = etf_full.iloc[length//2:, :].copy()
    
    # calculate signals
    for etf in (etf_t, etf_v):
        for price in ('high', 'low', 'close'):
            etf.loc[:, 'ema_{}_{}'.format(time_window, price)] = etf[price].ewm(span=time_window, adjust=False).mean()
        etf.loc[:, 'adr'] = etf['ema_{}_high'.format(time_window)] - etf['ema_{}_low'.format(time_window)]
        etf.loc[:, 'perc_range'] = \
            ((etf['ema_{}_high'.format(time_window)]-etf['ema_{}_close'.format(time_window)])*100)/etf['adr']
        
        for signal_type in ('long', 'short'):
            if signal_type == 'long':
                etf.loc[:, 'potential_signal'] = np.where(etf['perc_range'] > long_threshold, 1, 0)
            elif signal_type == 'short':
                etf.loc[:, 'potential_signal'] = np.where(etf['perc_range'] < short_threshold, 1, 0)
            etf.loc[:, 'previous_potential_signal'] = etf['potential_signal'].shift(1)
            etf['previous_potential_signal'].fillna(value=0, inplace=True)
            etf.loc[:, 'entry_{}'.format(signal_type)] = np.where(
                (etf['potential_signal']==1) & (etf['previous_potential_signal']==0), 1, 0
            )
            etf.loc[:, 'exit_{}'.format(signal_type)] = np.where(
                (etf['potential_signal']==0) & (etf['previous_potential_signal']==1), 1, 0
            )
            etf.drop(['potential_signal', 'previous_potential_signal'], axis=1, inplace=True)

    return etf_t, etf_v



class Backtester():
    def __init__(self, signals, price_label='close', available_money=10000):
        """
        *signals* - dictionary with symbols (key) and dataframes (values) with pricing data and enter/exit signals. 
        Column names for signals  are expected to be: entry_long, exit_long, entry_short, exit_short. Signals should 
        be bianries (0,1). If strategy is long/short only just insert 0 for the column.

        *price_label* - column name for the price which should be used in backtest
        """
        self.signals = self._prepare_signal(signals)
        self.price_label = price_label

        """
        initial_investment = int
        available_money = int
        account_value = int
        owned_shares = {'symbol': {'cnt': int, 'price': float, 'bought_ds': str}}
        trades = {'ds': details}
        """
        self.owned_shares = {}
        self.available_money = available_money

        # those will most probably be taken out to risk component
        self.fee_perc = 0.0038  # 0.38%
        self.min_fee = 4  # 4 PLN

    def _prepare_signal(self, signals):
        """Converts to expected dictionary form."""
        for k, v in signals.items():
            signals[k] = v.to_dict()
        return signals

    def _define_candidate(self, symbol, ds, entry_type):
        """Reutrns dictionary with purchease candidates and necessery keys."""
        return {
            'symbol': symbol,
            'entry_type': entry_type,
            'price': self.signals[symbol][self.price_label][ds]
        }

    def sell(self, symbol, exit_type):
        pass

    def buy(self, trx_details):
        print('trx_details: ', trx_details)
        pass

    def calculate_fee(self, transaction_value):
        """Calculates expected transaction fee."""
        fee = transaction_value * self.fee_perc
        if fee < self.min_fee:
            fee = self.min_fee
        return round(fee, 2)

    def buying_decisions(self, purchease_candidates):
        """
        Decides how much and of which shares to buy.

        Currently no complex decision making. Just buy as much as you can
        for the first encountered candidate from purchease_candidates.
        """
        symbols_to_buy = []
        available_money_at_time = self.available_money*1.0  #  multp. to create new obj.
        for candidate in purchease_candidates:
            price = candidate['price']
            shares_count = available_money_at_time // (price + (price*self.fee_perc))
            trx_value = shares_count*price
            expected_fee = self.calculate_fee(trx_value)
            symbols_to_buy.append({
                'symbol': candidate['symbol'],
                'entry_type': candidate['entry_type'],
                'shares_count': shares_count,
                'price': price,
                'trx_value': trx_value,
                'fee': expected_fee,
            })
            available_money_at_time -= (trx_value+expected_fee)
        return symbols_to_buy


    def summarize_day(self):
        pass

    def run(self, test_days=None):
        # days_with_symbols = {
        #     'day1': [symbol1, symbol2]
        #     'day2': [symbol1, symbol2, symbol3]
        #     'day3': [symbol1, symbol2, symbol3, symbol4]
        #     ...
        # }
        symbols_in_day = {}
        for sym_n, sym_v in self.signals.items():
            for ds in sym_v[self.price_label].keys():
                if ds in symbols_in_day:
                    symbols_in_day[ds].append(sym_n)
                else:
                    symbols_in_day[ds] = [sym_n]
        # days = symbols_in_day.keys() <- ordered
        days = sorted(symbols_in_day.keys())
        
        # trim no. of days if limit set
        if test_days:
            days = days[:test_days]

        for ds in days:
            print('in {}, following symbols are available: {}'.format(str(ds), symbols_in_day[ds]))


            # for symbol in symbols_in_day[ds]:
            #     anything_to_sell
            #         sell
            #         continue
            for symbol in self.owned_shares.keys():
                if self.signals[symbol]['exit_long'][ds] == 1:
                    print('checking if exit long signal for symbol: ', symbol)
                    self.sell(symbol, 'long')
                elif self.signals[symbol]['exit_short'][ds] == 1:
                    print('checking if exit short signal for symbol: ', symbol)
                    self.sell(symbol, 'short')

            # candidates = find_buying_candidates()
            purchease_candidates = []
            for sym in symbols_in_day[ds]:
                if self.signals[sym]['entry_long'][ds] == 1:
                    purchease_candidates.append(self._define_candidate(sym, ds, 'long'))
                elif self.signals[sym]['entry_short'][ds] == 1:
                    purchease_candidates.append(self._define_candidate(sym, ds, 'short'))

            print('     there are following candidates to buy: ', purchease_candidates)

            # symbols_to_buy = decide_what_and_how_much()
            symbols_to_buy = self.buying_decisions(purchease_candidates)

            # for symbol in symbols_to_buy:
            #     buy
            for trx_details in symbols_to_buy:
                self.buy(trx_details)

            # summarize day
            self.summarize_day()

        # df = convert_to_df()
        # return df


def main():
    sn1, sn2 = 'ETFW20L', 'ETFSP500'
    stock_1_test, stock_1_val = get_strategy_signals(sn1)
    stock_2_test, stock_2_val = get_strategy_signals(sn2)

    tdays = 15

    signals = {
        sn1: stock_1_test,
        sn2: stock_2_test,
    }

    print(signals['ETFW20L'].head(tdays))

    backtester = Backtester(signals)
    backtester.run(test_days=tdays)


if __name__ == '__main__':
    main()


"""
TODOs
- implement all the missing backtester.run components (buy, sell, summarize day)
- implement evaluation of the results (may be as a separate object)
- test your previous strategy (if gives same results)
- figure out how to change "buying_decisions" so that you can plug any logic to determine what and how much to buy
- test you previous strategy based on different buying_decisions settings
"""