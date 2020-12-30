# 3rd party
import pandas as pd

# custom
from commons import (
    setup_logging,
)

class AccountBankruptError(Exception):
    pass


class Backtester():
    def __init__(self, signals, price_label='close', init_capital=10000, logger=None, debug=False, 
                 position_sizer=None, stop_loss=False, auto_stop_loss=False, volatility_lb=14):
        """
        *signals* - dictionary with symbols (key) and dataframes (values) with pricing data and enter/exit signals. 
        Column names for signals  are expected to be: entry_long, exit_long, entry_short, exit_short. Signals should 
        be bianries (0,1). If strategy is long/short only just insert 0 for the column.

        *price_label*
            column name for the price which should be used in backtest
        *auto_stop_loss*
            automatically overwrites signals by setting % moving stop loss. should be double 
            (e.g. 0.01 fo 1%)
        """
        self.position_sizer = position_sizer
        self.price_label = price_label
        self.init_capital = init_capital
        self.stop_loss = stop_loss
        self.auto_stop_loss = auto_stop_loss
        self.volatility_lb = volatility_lb
        self.log = setup_logging(logger=logger, debug=debug)
        self.signals = self._prepare_signal(signals)

    def run(self, test_days=None):
        self._reset_backtest_state()

        self.log.debug('Starting backtest. Initial capital:{}, Available symbols: {}'.format(
            self._available_money, list(self.signals.keys())
        ))

        symbols_in_day = {}
        symbols_next_days = {} # {sym: {day1: day2, day2:day3, ...}, ...}
        for sym_n, sym_v in self.signals.items():
            _dss = sorted(sym_v[self.price_label].keys())
            symbols_next_days[sym_n] = {val: _dss[idx+1] for idx, val in enumerate(_dss[:-1])}
            for ds in _dss:
                if ds in symbols_in_day:
                    symbols_in_day[ds].append(sym_n)
                else:
                    symbols_in_day[ds] = [sym_n]
        days = sorted(symbols_in_day.keys())
        if test_days:
            days = days[:test_days]
        else:
            days = days

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
            available_owned_shares = []
            for symbol in owned_shares:
                # safe check if missing ds for given owned symbol
                if not symbol in symbols_in_day[ds]:
                    continue
                current_sym_price = self._get_price(symbol, ds)
                self.log.debug('\t+ Checking exit signal for: ' + symbol)
                _sold = 0
                # 0) check if stop loss
                if (self.stop_loss == True) or (self.auto_stop_loss != False):
                    stop_loss_price = self.signals[symbol]['stop_loss'][ds]
                    trade_type = self._trades[self._owned_shares[symbol]['trx_id']]['type']
                    if (trade_type == 'long') and (current_sym_price <= stop_loss_price):
                        self.log.debug('\t\t LONG STOP LOSS TRIGGERED - EXITING')
                        self._sell(symbol, stop_loss_price, ds, 'long')
                        _sold = 1
                    elif (trade_type == 'short') and (current_sym_price >= stop_loss_price):
                        self.log.debug('\t\t SHORT STOP LOSS TRIGGERED - EXITING')
                        self._sell(symbol, stop_loss_price, ds, 'short')
                        _sold = 1
                # 1) if stop loss does not exists: check usual exit signal
                # 2) in case stop loss exists but it was not triggered: check usual exit signal
                if (self.signals[symbol]['exit_long'][ds] == 1) and (_sold == 0):
                    self.log.debug('\t\t EXIT LONG')
                    self._sell(symbol, current_sym_price, ds, 'long')
                    _sold = 1
                elif (self.signals[symbol]['exit_short'][ds] == 1) and (_sold == 0):
                    self.log.debug('\t\t EXIT SHORT')
                    self._sell(symbol, current_sym_price, ds, 'short')
                    _sold = 1
                if _sold == 0:
                    available_owned_shares.append(symbol)
                    self.log.debug('\t+ Not exiting from: ' + symbol)
                elif (_sold == 1) and (self.auto_stop_loss != False):
                    self._auto_stop_loss_tracker.pop(symbol, None)

            if self._available_money < 0:
                raise AccountBankruptError(
                    "Account bankrupted! Money after sells is: {}. Backtester cannot run anymore!".format(
                        self._available_money
                    ))

            self.log.debug('\t[-- SELL END --]')
            self.log.debug('\t[-- BUY START --]')
            purchease_candidates = []
            for sym in symbols_in_day[ds]:
                cur_price = self._get_price(sym, ds)
                # set up back-up price for all available symbols in given day
                price = self._backup_prices[sym] = (cur_price, ds)
                if self.signals[sym]['entry_long'][ds] == 1:
                    purchease_candidates.append(self._define_candidate(cur_price, sym, ds, 'long'))
                elif self.signals[sym]['entry_short'][ds] == 1:
                    purchease_candidates.append(self._define_candidate(cur_price, sym, ds, 'short'))
            if purchease_candidates == []:
                self.log.debug('\t\tNo candidates to buy.')
            else:
                self.log.debug('\tCandidates to buy: {}'.format([c['symbol'] for c in purchease_candidates]))

            capital_at_time = self._available_money + self._calculate_account_value(ds) + self._get_money_from_short()
            symbols_to_buy = self.position_sizer.decide_what_to_buy(
                self._available_money*1.0,  # multplication is to create new object instead of using actual pointer
                purchease_candidates,
                capital = capital_at_time,
                volatility = {
                    c['symbol']: self.signals[c['symbol']]['volatility'][ds] 
                    for c in purchease_candidates
                }
            )
            for trx_details in symbols_to_buy:
                self._buy(trx_details, ds)
                available_owned_shares.append(trx_details['symbol'])
            self.log.debug('\t[--  BUY END --]')

            # update auto_stop_loss for available owned shares. it will be applied to existing next day 
            # based on data from current day
            if self.auto_stop_loss != False:
                for sym in available_owned_shares:
                    if (sym not in symbols_in_day[ds]) or (ds not in symbols_next_days[sym]):
                        continue
                    try:
                        asl = self._update_auto_stop_loss(
                            sym, self._get_price(sym, ds), ds, symbols_next_days[sym][ds]
                        )
                        self.log.debug(f'\t Updated SL [{sym}]: {symbols_next_days[sym][ds]}: {asl}')
                    except IndexError:
                        # this will be the case only once (at the last processed day)
                        pass

            self._summarize_day(ds)
        return self._run_output(), self._trades

    def _prepare_signal(self, signals):
        """Converts to expected dictionary form."""
        self.log.debug('Prepareing signals')
        _signals = {}
        for k, df in signals.items():
            # ignore if empty dataframe
            if df.shape[0] == 0:
                continue
            # initialize stop_loss column if needed
            if 'stop_loss' not in df.columns:
                df.loc[:, 'stop_loss'] = None
            # calculate volatility column with appropriate lag
            if 'volatility' not in df.columns:
                df.loc[:, 'volatility'] = df[self.price_label].shift().rolling(self.volatility_lb).std()
                df['volatility'].fillna(0, inplace=True)
            _signals[k] = df.to_dict()
        self.log.debug('Signals ready.')
        return _signals

    def _reset_backtest_state(self):
        """
        Resets/Initializes all attributes used during backtest run.
        """
        self._owned_shares = {}
        self._available_money = self.init_capital
        self._money_from_short = {}
        self._trades = {}
        self._account_value = {}
        self._net_account_value = {}
        self._rate_of_return = {}
        self._backup_prices = {}
        self._auto_stop_loss_tracker = {}

    def _sell(self, symbol, price, ds, exit_type):
        """Selling procedure"""
        shares_count = self._owned_shares[symbol]['cnt']
        fee = self.position_sizer.calculate_fee(abs(shares_count)*price)
        trx_value = (abs(shares_count)*price)
        
        trx_id = self._owned_shares[symbol]['trx_id']

        self.log.debug('\t\tSelling {} (Transaction id: {})'.format(symbol, trx_id))
        self.log.debug('\t\t\tNo. of sold shares: ' + str(int(shares_count)))
        self.log.debug('\t\t\tSell price: ' + str(price))
        self.log.debug('\t\t\tFee: ' + str(fee))
        self.log.debug('\t\t\tTransaction value (no fee): ' + str(trx_value))
        self.log.debug('\t\t\tTransaction value (gross): ' + str(trx_value - fee))

        buy_trx_value_with_fee = self._trades[trx_id]['trx_value_with_fee']
        
        if exit_type == 'long':
            sell_trx_value_with_fee = trx_value - fee
            profit = sell_trx_value_with_fee - buy_trx_value_with_fee
            self._available_money += sell_trx_value_with_fee
        elif exit_type == 'short':
            sell_trx_value_with_fee = trx_value + fee
            profit = buy_trx_value_with_fee - sell_trx_value_with_fee
            self._available_money += self._money_from_short[trx_id]
            self._money_from_short.pop(trx_id)
            self._available_money -= sell_trx_value_with_fee
  
        self.log.debug('\t\tAvailable money after selling: ' + str(self._available_money))
        
        self._trades[trx_id].update({
            'sell_ds': ds,
            'sell_value_no_fee': trx_value,
            'sell_value_with_fee': sell_trx_value_with_fee,
            'profit': round(profit, 2)
        })
        self._owned_shares.pop(symbol)

    def _buy(self, trx, ds):
        """Buying procedure"""
        if self._owned_shares.get(trx['symbol']):
            raise ValueError(
                '[{}] Trying to buy {} of {}. You currenlty own this symbol.\
                Buying additional/partial selling is currently not supported'.format(
                    ds, trx['entry_type'], trx['symbol']
                )
            )
        if trx['shares_count'] == 0:
            raise ValueError(
                f'Trying to buy 0 shares. It should not be possible'
            )

        trx_id = '_'.join((str(ds)[:10], trx['symbol'], trx['entry_type']))
        self.log.debug('\t\tBuying {} (Transaction id: {})'.format(trx['symbol'], trx_id))
        
        if trx['entry_type'] == 'long':
            trx_value_with_fee = trx['trx_value'] + trx['fee'] # i need to spend
            self._owned_shares[trx['symbol']] = {'cnt': trx['shares_count']}
            self._available_money -= trx_value_with_fee
                    
        elif trx['entry_type'] == 'short':
            trx_value_with_fee = trx['trx_value'] - trx['fee'] # i will get
            self._owned_shares[trx['symbol']] = {'cnt': -trx['shares_count']}
            self._available_money -= trx['fee']
            self._money_from_short[trx_id] = trx['trx_value']

        self._available_money = round(self._available_money, 2)

        self._owned_shares[trx['symbol']]['trx_id'] = trx_id
        self._trades[trx_id] = {
            'buy_ds': ds,
            'type': trx['entry_type'],
            'trx_value_no_fee': trx['trx_value'],
            'trx_value_with_fee': trx_value_with_fee,
        }

        self.log.debug('\t\t\tNo. of bought shares: ' + str(int(trx['shares_count'])))
        self.log.debug('\t\t\tBuy price: ' + str(trx['price']))
        self.log.debug('\t\t\tFee: ' + str(trx['fee']))
        self.log.debug('\t\t\tTransaction value (no fee): ' + str(trx['trx_value']))
        self.log.debug('\t\t\tTransaction value (gross): ' + str(trx_value_with_fee))
        self.log.debug('\t\tAvailable money after buying: ' + str(self._available_money))
        if trx['entry_type'] == 'short':
            self.log.debug('\t\tMoney from short sell: ' + str(self._money_from_short[trx_id]))

    def _define_candidate(self, price, symbol, ds, entry_type):
        """
        Reutrns dictionary with purchease candidates and necessery keys.
        Handles setting up value for auto_stop_loss.
        """
        if self.auto_stop_loss != False:
            stop_loss = self._calc_auto_sl(price, entry_type)
        elif self.stop_loss:
            stop_loss = self.signals[symbol].get('stop_loss', None)      
        else:
            stop_loss = None
        return self.position_sizer.define_candidate(
            symbol=symbol,
            entry_type=entry_type,
            price=price,
            stop_loss=stop_loss,
        )

    def _calc_auto_sl(self, price, entry_type):
        if entry_type == 'long':
            return price - (price*self.auto_stop_loss)
        elif entry_type == 'short':
            return price + (price*self.auto_stop_loss)

    def _calculate_account_value(self, ds):
        _account_value = 0
        for symbol, vals in self._owned_shares.items():
            _account_value += vals['cnt'] * self._get_price(symbol, ds)
        return _account_value

    def _get_money_from_short(self):
        return sum([m for m in self._money_from_short.values()])

    def _get_price(self, symbol, ds):
        """
        Do not use directly. If ds is not available it uses backup price. This backup price may not be
        avialable. As its being constantly overwritten it also depends on backtest execution.
        """
        price = self.signals[symbol][self.price_label].get(ds)
        if price == None:
            price = self._backup_prices[symbol][0]
        return price

    def _update_auto_stop_loss(self, symbol, price, cur_ds, next_ds):
        """
        curr_sl_ref_price is the price from which actual SL was calculated. It's not SL itself.
        Also, note that SL is set up for ds+1 day.
        """
        if self._owned_shares[symbol]['cnt'] > 0:
            entry_type = 'long'
        else:
            entry_type = 'short'
        curr_sl_ref_price = self._auto_stop_loss_tracker.get(symbol, 0)
        if price > curr_sl_ref_price:
            self._auto_stop_loss_tracker[symbol] = price
            stop_loss = self._calc_auto_sl(price, entry_type)
        else:
            stop_loss = self.signals[symbol]['stop_loss'][cur_ds]
        self.signals[symbol]['stop_loss'][next_ds] = stop_loss
        return stop_loss

    def _summarize_day(self, ds):
        """Sets up summaries after finished session day."""
        self.log.debug('[ SUMMARIZE SESSION {} ]'.format(str(ds)[:10]))
        _account_value = self._calculate_account_value(ds)
        # account value (can be negative) + avaiable money + any borrowed moneny
        nav = _account_value + self._available_money + self._get_money_from_short()
        self._account_value[ds] = _account_value
        self._net_account_value[ds] = nav
        self._rate_of_return[ds] = ((nav-self.init_capital)/self.init_capital)*100

        self.log.debug('Available money is: ' + str(self._available_money))
        self.log.debug('Shares: ' + ', '.join(sorted(['{}: {}'.format(k, v['cnt']) for k,v in self._owned_shares.items()])))
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


class SimpleBacktest():
    def __init__(self, df=None, position_label='position', price_label='close', init_capital=10000):
        """
        Simplified version of Backtest. Requires only price and position labels in df. Does not handle
        transaction costs, positions sizing bankruptcy etc Results from it will be too positive and not 
        realizable in real life.

        It is more efficient that Backtester though.
        """
        self.df = df.copy()
        self.position_label = position_label
        self.price_label = price_label
        self.init_capital = init_capital
    
    def run(self):
        self.df.loc[:, 'pct_change'] = self.df[self.price_label].pct_change()
        self.df.loc[:, 'nav'] = self.init_capital * (
            1 + ( self.df[self.position_label].shift(1) * self.df['pct_change'])
        ).cumprod()
        if any(self.df['nav'] < 0):
            raise AccountBankruptError(
                    "Account bankrupted! SimpleBacktest encountered negative Net Account Value"
                )
        return self.df[['nav']]


def test_backtest_normal_vs_simple():
    import gpw_data
    import position_size
    import results
    import rules
    import signal_generator

    test_config = {
        'rules': [
            {
                'id': 'r1',
                'type': 'simple',
                'ts': 'adj_close',
                'lookback': 28,
                'params': {},
                'func': rules.moving_average,
            },
        ],
        'strategy': {
            'type': 'fixed',
            'strategy_rules': ['r1'],
            'strategy_id': 'simple_test'
        }
    }

    data_collector = gpw_data.GPWData()
    symbol = 'CCC'
    symbol_data = data_collector.load(symbols=symbol, from_csv=True, df=True)
    symbol_data = data_collector.detrend(symbol_data)
    
    sg = signal_generator.SignalGenerator(
        df = symbol_data,
        config = test_config,
    )
    signals = sg.generate()
    tester = Backtester(
        {symbol: signals},
        position_sizer=position_size.MaxFirstEncountered(fee_perc=0, min_fee=0),
    )

    tester_results, tester_trades = tester.run()
    tester_results.loc[:, 'daily_returns'] = results.get_daily_returns(tester_results)
    print('For standard tester, avg daily returns are: ', tester_results['daily_returns'].mean())
    print(tester_results.tail(30))



    simple_tester = SimpleBacktest(df=signals)
    simple_tester_results = simple_tester.run()
    simple_tester_results.loc[:, 'daily_returns'] = results.get_daily_returns(simple_tester_results)
    print('For simplified tester, avg daily returns are: ', simple_tester_results['daily_returns'].mean())
    print(simple_tester_results.tail(30))


if __name__ == '__main__':
    test_backtest_normal_vs_simple()
