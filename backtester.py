# build-in
import logging

# 3rd party
import numpy as np
import pandas as pd

# custom
from gpw_data import GPWData

from position_size import (
    MaxFirstEncountered,
)

from commons import (
    setup_logging,
    get_parser,
)



def get_strategy_signals(symbols):
    # TODO(2019-02-09) This strategy here is just for testing purposes... Will remove it soon.
    data = GPWData()
    etfs = data.load(symbols=symbols)
    if not isinstance(etfs, dict):
        etfs = {symbols: etfs}

    # strategy params (should come as params but hardcoded here for simplicity)
    time_window=3
    long_threshold=35
    short_threshold=30
    
    # calculate signals
    for sym, etf in etfs.items():
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

    etfs_t, etfs_v = data.split_into_subsets(etfs, 0.5)

    return etfs_t, etfs_v



class Backtester():
    def __init__(self, signals, price_label='close', init_capital=10000, logger=None, debug=False, 
                 position_sizer=None):
        """
        *signals* - dictionary with symbols (key) and dataframes (values) with pricing data and enter/exit signals. 
        Column names for signals  are expected to be: entry_long, exit_long, entry_short, exit_short. Signals should 
        be bianries (0,1). If strategy is long/short only just insert 0 for the column.

        *price_label* - column name for the price which should be used in backtest
        """
        self.log = setup_logging(logger=logger, debug=debug)
        self.signals = self._prepare_signal(signals)
        self.position_sizer = position_sizer
        self.price_label = price_label
        self.init_capital = init_capital

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

        for idx, ds in enumerate(days):
            self.log.debug('['+15*'-'+str(ds)[0:10]+15*'-'+']')
            self.log.debug('\tSymbols available in given session: ' + str(symbols_in_day[ds]))

            owned_shares = list(self._owned_shares.keys())
            self.log.debug('\t[-- SELL START --]')
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
                    self._sell(symbol, self.signals[symbol][self.price_label], ds, 'long')
                elif self.signals[symbol]['exit_short'][ds] == 1:
                    self.log.debug('\t\t EXIT SHORT')
                    self._sell(symbol, self.signals[symbol][self.price_label], ds, 'short')
                else:
                    self.log.debug('\t+ Not exiting from: ' + symbol)
            self.log.debug('\t[-- SELL END --]')
            self.log.debug('\t[-- BUY START --]')
            purchease_candidates = []
            for sym in symbols_in_day[ds]:
                if self.signals[sym]['entry_long'][ds] == 1:
                    purchease_candidates.append(self._define_candidate(sym, ds, 'long'))
                elif self.signals[sym]['entry_short'][ds] == 1:
                    purchease_candidates.append(self._define_candidate(sym, ds, 'short'))
            if purchease_candidates == []:
                self.log.debug('\t\tNo candidates to buy.')
            else:
                self.log.debug('\tCandidates to buy: {}'.format([c['symbol'] for c in purchease_candidates]))

            symbols_to_buy = self.position_sizer.decide_what_to_buy(
                self._available_money*1.0,  # multplication is to create new object instead of using actual pointer
                purchease_candidates,
                capital = self._net_account_value[days[idx-1]] if idx > 0 else self.init_capital
            )

            for trx_details in symbols_to_buy:
                self._buy(trx_details, ds)
            self.log.debug('\t[--  BUY END --]')

            self._summarize_day(ds)

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
        self._money_from_short = {}
        self._trades = {}
        self._account_value = {}
        self._net_account_value = {}
        self._rate_of_return = {}
        self._backup_close_prices = {}

    def _sell(self, symbol, prices, ds, exit_type):
        """Selling procedure"""
        price = prices[ds]
        shares_count = self._owned_shares[symbol]['cnt']
        fee = self.position_sizer.calculate_fee(abs(shares_count)*price)
        trx_value = (abs(shares_count)*price)
        trx_value_gross = trx_value - fee

        trx_id = self._owned_shares[symbol]['trx_id']

        self.log.debug('\t\tSelling {} (Transaction id: {})'.format(symbol, trx_id))
        self.log.debug('\t\t\tNo. of sold shares: ' + str(int(shares_count)))
        self.log.debug('\t\t\tSell price: ' + str(price))
        self.log.debug('\t\t\tFee: ' + str(fee))
        self.log.debug('\t\t\tTransaction value (no fee): ' + str(trx_value))
        self.log.debug('\t\t\tTransaction value (gross): ' + str(trx_value - fee))
        
        if exit_type == 'short':
            # have to return whole borrowed money first
            self._available_money -= self._money_from_short[trx_id] 
        self._available_money += trx_value_gross
        """
        trx_value_gross jest ujemne...

        tutaj musze 
            1) oddac pieniadze 
            2) wziac co zostalo albo zabrac z tego co mam zeby wyszlo na zero
        
        from_sale = abs(trx_value)-fee 

        1) available_money = available_money - money_from_short   <--- oddaje
        2) available_money = available_money + from_sale  <--- biore to co ze sprzedazy
        """

        self.log.debug('\t\tAvailable money after selling: ' + str(self._available_money))
        if self._available_money < 0:
            raise ValueError(
                "Account bankrupted! Money available after sell is: {}. Backtester cannot run anymore!".format(self._available_money)
            )
        
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
            trx_value_gross = abs(trx['trx_value']) + trx['fee']
            self._owned_shares[trx['symbol']] = {'cnt': -trx['shares_count']}
            self._available_money -= trx['fee']

            self._money_from_short[trx_id] = trx['trx_value']

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
        if trx['entry_type'] == 'short':
            self.log.debug('\t\tMoney from short sell: ' + str(self._money_from_short[trx_id]))

    def _define_candidate(self, symbol, ds, entry_type):
        """Reutrns dictionary with purchease candidates and necessery keys."""
        return {
            'symbol': symbol,
            'entry_type': entry_type,
            'price': self.signals[symbol][self.price_label][ds]
        }

    def _summarize_day(self, ds):
        """Sets up summaries after finished session day."""
        self.log.debug('[ SUMMARIZE SESSION {} ]'.format(str(ds)[:10]))
        _account_value = 0
        for symbol, vals in self._owned_shares.items():
            try:
                price = self.signals[symbol][self.price_label][ds]
            except KeyError:
                # in case of missing ds in symbol take previous price value
                price, price_ds = self._backup_close_prices[symbol]
                self.log.warning('\t\t!!! Using backup price from {} for {} as there was no data for it at {} !!!'.format(
                    price_ds, symbol, ds
                ))

            _account_value += vals['cnt'] * price
            self._backup_close_prices[symbol] = (price, ds)

        # account value (can be negative) + avaiable money + any borrowed moneny
        nav = _account_value + self._available_money + sum([m for m in self._money_from_short[trx_id].values()]) 
        self._account_value[ds] = _account_value
        self._net_account_value[ds] = nav
        self._rate_of_return[ds] = ((nav-self.init_capital)/self.init_capital)*100

        self.log.debug('Available money is: ' + str(self._available_money))
        self.log.debug('Shares: ' + ','.join(sorted(['{}: {}'.format(k, v['cnt']) for k,v in self._owned_shares.items()])))
        self.log.debug('Net Account Value is: ' + str(nav))
        self.log.debug('Rate of return: ' + str(self._rate_of_return[ds]))

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
    # symbols = 'ETFW20L'
    symbols = 'ETFSP500'
    # symbols = ['ETFW20L', 'ETFSP500']
    test_signals, validation_signals = get_strategy_signals(symbols)

    position_sizer = MaxFirstEncountered(debug=debug, sort_type='cheapest')
    backtester = Backtester(test_signals, position_sizer=position_sizer, debug=debug)
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
- test you previous strategy with 2 symbols (previously I was getting a lot of warnings due to wrong split)
    + fix and add test for following:
        On 2011-06-09 I'm entering short for ETFSP500. As this is short sell - it adds me money to account. 
        After that buying short I have 18679 available money.

        On 2011-06-10 I'm getting signal for ETFW20L to go long. As my available money is 18679 - I'm buiying a lot of stocks
        
        On 2011-06-15 I'm exiting from short on ETFSP500 -> getting bankrupt!!! 

    + test for bankruptcy (does it terminates properly process instead of moving formard)

- add more test for problems which occured during testing strategy for 2 symbols
- add test with 3 where you own 1, short 2nd and buy 3rd
- test strategy with 3 symbols... all should work good

- test you previous strategy based on different buying_decisions (new position sizer)

- clean code, write any more tests you think

- summarize recent on strategy 1:
    -> move notebooks
    -> convert them to pure research
    -> remove testing of straetegy 1 from backtester codes

- add optimization pipeline!!!
"""