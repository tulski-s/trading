# build-in
import logging

# 3rd party
import numpy as np
import pandas as pd

# custom
from gpw_data import GPWData
from commons import (
    setup_logging,
    get_parser,
)



def get_strategy_signals(symbol):
    # TODO(2019-02-09) This strategy here is just for testing purposes... Will remove it soon.
    data = GPWData()
    etf_full = data.load(symbols=symbol)

    # strategy params (should come as params but hardcoded here for simplicity)
    time_window=3
    long_threshold=35
    short_threshold=30

    # split data into optimize/validate
    length = len(etf_full.index)

    # this splitting to half is actually pretty bad. as one can have mutiple timspans
    # for example, half of ETF woth 10y data is 5y and half of 2y history is 1y
    # if that is used for stock universe with multiple stocks then dates will not overlapp as they should
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

    # print('na chwile: ', etf_t.head(5))

    return etf_t, etf_v



class Backtester():
    def __init__(self, signals, price_label='close', init_capital=10000, logger=None, debug=False):
        """
        *signals* - dictionary with symbols (key) and dataframes (values) with pricing data and enter/exit signals. 
        Column names for signals  are expected to be: entry_long, exit_long, entry_short, exit_short. Signals should 
        be bianries (0,1). If strategy is long/short only just insert 0 for the column.

        *price_label* - column name for the price which should be used in backtest
        """
        self.log = setup_logging(logger=logger, debug=debug)
        self.signals = self._prepare_signal(signals)
        self.price_label = price_label
        self.init_capital = init_capital

        # those will most probably be taken out to risk component
        self.fee_perc = 0.0038  # 0.38%
        self.min_fee = 4  # 4 PLN

    def buying_decisions(self, purchease_candidates):
        """
        Decides how much and of which shares to buy.

        Currently no complex decision making. Just buy as much as you can
        for the first encountered candidate from purchease_candidates.
        """
        symbols_to_buy = []
        available_money_at_time = self._available_money*1.0  #  multp. to create new obj.
        for candidate in purchease_candidates:
            self.log.debug('\t+ Deciding if buy {} - {}'.format(candidate['symbol'], candidate['entry_type']))
            price = candidate['price']
            shares_count = available_money_at_time // (price + (price*self.fee_perc))
            if shares_count == 0:
                continue
            trx_value = shares_count*price
            expected_fee = self.calculate_fee(trx_value)
            self.log.debug('\t\tWill buy {} shares of {}'.format(shares_count, candidate['symbol']))
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

    def calculate_fee(self, transaction_value):
        """Calculates expected transaction fee."""
        fee = transaction_value * self.fee_perc
        if fee < self.min_fee:
            fee = self.min_fee
        return round(fee, 2)

    def run(self, test_days=None):
        self._reset_backtest_state()

        self.log.debug('Starting backtest. Initial capital:{}, Available symbols: {}'.format(
            self._available_money, list(self.signals.keys())
        ))

        symbols_in_day = {}
        for sym_n, sym_v in self.signals.items():
            for ds in sym_v[self.price_label].keys():
                if ds in symbols_in_day:
                    symbols_in_day[ds].append(sym_n)
                else:
                    symbols_in_day[ds] = [sym_n]
        days = sorted(symbols_in_day.keys())
        
        if test_days:
            days = days[:test_days]

        for ds in days:
            self.log.debug('['+15*'-'+str(ds)[0:10]+15*'-'+']')
            self.log.debug('\tAvailable symbols: ' + str(symbols_in_day[ds]))

            owned_shares = list(self._owned_shares.keys())
            self.log.debug('\t-- SELL START --')
            if len(owned_shares) == 0:
                self.log.debug('\t\tNo shares owned. Nothing to sell.')
            else:
                self.log.debug(
                    '\tOwned shares: ' + ', '.join('{}={}'.format(s, int(self._owned_shares[s]['cnt'])) 
                        for s in sorted(owned_shares))
                )
            for symbol in owned_shares:
                # safe check if missing ds for given owned symbol
                if not symbol in symbols_in_day[ds]:
                    continue
                self.log.debug('\t+ Checking exit signal for: ' + symbol)
                if self.signals[symbol]['exit_long'][ds] == 1:
                    self.log.debug('\t\t EXIT LONG')
                    self._sell(symbol, self.signals[symbol][self.price_label], ds)
                elif self.signals[symbol]['exit_short'][ds] == 1:
                    self.log.debug('\t\t EXIT SHORT')
                    self._sell(symbol, self.signals[symbol][self.price_label], ds)
            self.log.debug('\t-- SELL END --')
            self.log.debug('\t-- BUY START --')
            purchease_candidates = []
            for sym in symbols_in_day[ds]:
                if self.signals[sym]['entry_long'][ds] == 1:
                    purchease_candidates.append(self._define_candidate(sym, ds, 'long'))
                elif self.signals[sym]['entry_short'][ds] == 1:
                    purchease_candidates.append(self._define_candidate(sym, ds, 'short'))
            if purchease_candidates == []:
                self.log.debug('\t\tNo candidates to buy.')
            symbols_to_buy = self.buying_decisions(purchease_candidates)

            for trx_details in symbols_to_buy:
                self._buy(trx_details, ds)
            self.log.debug('\t--  BUY END --')

            self._summarize_day(ds)

            # self.log.debug('-> NAV after session({}) is : {}'.format(ds, self._net_account_value[ds]))

        return self._run_output(), self._trades

    def _prepare_signal(self, signals):
        """Converts to expected dictionary form."""
        for k, v in signals.items():
            signals[k] = v.to_dict()
        return signals

    def _reset_backtest_state(self):
        """Resets all attributes used during backtest run."""
        self._owned_shares = {}
        self._available_money = self.init_capital
        self._trades = {}
        self._account_value = {}
        self._net_account_value = {}
        self._rate_of_return = {}
        self._backup_close_prices = {}

    def _sell(self, symbol, prices, ds):
        """Selling procedure"""
        price = prices[ds]
        shares_count = self._owned_shares[symbol]['cnt']
        fee = self.calculate_fee(abs(shares_count)*price)
        trx_value = (shares_count*price)
        trx_value_gross = trx_value - fee

        self.log.debug('\t\tSelling {} (Transaction id: {})'.format(symbol, self._owned_shares[symbol]['trx_id']))
        self.log.debug('\t\t\tNo. of sold shares: ' + str(int(shares_count)))
        self.log.debug('\t\t\tSell price: ' + str(price))
        self.log.debug('\t\t\tFee: ' + str(fee))
        self.log.debug('\t\t\tTransaction value (no fee): ' + str(trx_value))
        self.log.debug('\t\t\tTransaction value (gross): ' + str(trx_value - fee))
        
        self._available_money += trx_value_gross

        self.log.debug('\t\tAvailable money after selling: ' + str(self._available_money))
        
        self._trades[self._owned_shares[symbol]['trx_id']].update({
            'sell_ds': ds,
            'sell_value_no_fee': trx_value,
            'sell_value_gross': trx_value_gross,
        })
        self._owned_shares.pop(symbol)

    def _buy(self, trx, ds):
        """Buying procedure"""
        trx_id = '_'.join((str(ds)[:10], trx['symbol'], trx['entry_type']))
        self.log.debug('\t\tBuying {} (Transaction id: {})'.format(trx['symbol'], trx_id))
        
        if trx['entry_type'] == 'long':
            trx_value_gross = trx['trx_value'] + trx['fee']
            self._owned_shares[trx['symbol']] = {'cnt': trx['shares_count']}
            self._available_money -= trx_value_gross
                    
        elif trx['entry_type'] == 'short':
            trx_value_gross = trx['trx_value'] - trx['fee']
            self._owned_shares[trx['symbol']] = {'cnt': -trx['shares_count']}
            self._available_money += trx_value_gross

        self._owned_shares[trx['symbol']]['trx_id'] = trx_id
        self._trades[trx_id] = {
            'buy_ds': ds,
            'type': trx['entry_type'],
            'trx_value_no_fee': trx['trx_value'],
            'trx_value_gross': trx_value_gross,
        }

        self.log.debug('\t\t\tNo. of bought shares: ' + str(int(trx['shares_count'])))
        self.log.debug('\t\t\tBuy price: ' + str(trx['price']))
        self.log.debug('\t\t\tFee: ' + str(trx['fee']))
        self.log.debug('\t\t\tTransaction value (no fee): ' + str(trx['trx_value']))
        self.log.debug('\t\t\tTransaction value (gross): ' + str(trx_value_gross))
        self.log.debug('\t\tAvailable money after buying: ' + str(self._available_money))

    def _define_candidate(self, symbol, ds, entry_type):
        """Reutrns dictionary with purchease candidates and necessery keys."""
        return {
            'symbol': symbol,
            'entry_type': entry_type,
            'price': self.signals[symbol][self.price_label][ds]
        }

    def _summarize_day(self, ds):
        """Sets up summaries after finished session day."""
        self.log.debug('[SUMMARIZE SESSION {}]'.format(str(ds)[:10]))
        _account_value = 0
        for symbol, vals in self._owned_shares.items():
            try:
                price = self.signals[symbol][self.price_label][ds]
            except KeyError:
                # in case of missing ds in symbol take previous price value
                price, price_ds = self._backup_close_prices[symbol]
                # TODO(slaw): log here that there was missing date with status such that it shows even if DEBUG is off
                # self.log.debug(30*' ', '!!! Using backup price from {} for {} as there was no data for it at {} !!!'.format(
                #     price_ds, symbol, ds
                # ))
                
            _account_value += vals['cnt'] * price
            self._backup_close_prices[symbol] = (price, ds)


        nav = _account_value + self._available_money
        self._account_value[ds] = _account_value
        self._net_account_value[ds] = nav
        self._rate_of_return[ds] = ((nav-self.init_capital)/self.init_capital)*100

    def _run_output(self):
        """
        Aggregates results from backtester run and outputs it as a DataFrame
        """
        df = pd.DataFrame()
        idx = list(self._account_value.keys())
        results = (
            (self._account_value, 'account_value'),
            (self._net_account_value, 'nav'),
            (self._rate_of_return, 'rate_of_return')
        )
        for d, col in results:
            temp_df = pd.DataFrame(list(d.items()), index=idx, columns=['ds', col])
            temp_df.drop('ds', axis=1, inplace=True)
            df = pd.concat([df, temp_df], axis=1)
        return df


def run_test_strategy(days=-1, debug=False):
    sn1, sn2 = 'ETFW20L', 'ETFSP500'
    stock_1_test, stock_1_val = get_strategy_signals(sn1)
    stock_2_test, stock_2_val = get_strategy_signals(sn2)

    signals = {
        sn1: stock_1_test,
        # sn2: stock_2_test,
    }

    # print(signals['ETFW20L'].head(tdays))

    backtester = Backtester(signals, debug=debug)
    if days == -1:
        results, trades = backtester.run()
    else:
        results, trades = backtester.run(test_days=days)

    return results, trades 


if __name__ == '__main__':
    parser = get_parser()
    parser.add_argument('--days', '-d', type=int, default=-1, help='number of days to run backtester for')
    args = parser.parse_args()
    run_test_strategy(args.days, args.debug)


"""
TODOs
- test your previous strategy - if evaluation results gives the same results, if not - find out why
- figure out how to change "buying_decisions" so that you can plug any logic to determine what and how much to buy
- test you previous strategy based on different buying_decisions settings
- better logic for handling universes (finding overlapping periods, spliting into test/validation, etc.)

- clean code, enhence logging, write tests
    https://docs.python.org/3/howto/logging.html
    https://docs.python-guide.org/writing/logging/
"""