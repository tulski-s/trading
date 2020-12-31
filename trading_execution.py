# built-in
import datetime
import sys
import time

sys.path.insert(0, '/Users/slaw/osobiste/trading')

# 3rd party
import pytz

# custom
import commons
from lse_data import LSEData
from signal_generator import SignalGenerator
from strategies.strategy_4 import long_only_s4_config
import strategies.helpers as helpers
from ib_api import IBAPIApp
from ftse_symbols import ftse_100_to_ib_map
from position_size import FixedRisk


class TradingExecutor():
    """
    As it is right now it is NOT some sort of generic executor. Right now it is built for and supports
    very specific trading strategy:
        - long only
        - stocks only
        - in GBP
        - predefined universe
        - LSE and trading hours
        - fixed stop loss % (`stop_loss_perc` is in %, i.e. 1% -> 1)
        - predefined postion sizing
        - you can not buy same symbol (order more shares of sth you alredy own)
        - etc.
    """
    def __init__(self, pricing_data_path='./pricing_data', load_csv=False, logger=None, debug=False, 
                 signal_config=None, signal_lookback=None, ib_port=None, ib_client=666, stop_loss_perc=1.5,
                 position_sizer=None):
        self.today = str(self._now().date())
        self.log = commons.setup_logging(logger=logger, debug=debug)
        self.pricing_data_path = pricing_data_path
        self.load_csv = load_csv
        self.signal_config = signal_config
        self.signal_lookback = signal_lookback
        self.stop_loss_perc = stop_loss_perc/100.0
        self.position_sizer = position_sizer
        # start IB App
        self.ib_app = IBAPIApp(
            port=ib_port,
            clientId=ib_client,
        )
        self._check_ib_env()

    def trade(self, ignore_xe_check=False, ignore_tod_check=False, ignore_sa_check=False):
        # Check if exchange is open
        if ignore_xe_check == False:
            exchange_is_open = self.check_if_trade_open()
            if not exchange_is_open:
                self.log.debug('Exchange is closed. No trading')
                return None
        else:
            self.log.debug('Ignore check if exchange is open')

        # Start trading closer to the end of the current session
        # That is scenario closest to the one from the backtest
        if ignore_tod_check == False:
            while True:
                if self._now().hour >= 15:
                    break
                else:
                    self.log.debug('Will start trading after 3pm London time. Nothing to do yet')
                    time.sleep(60)
        else:
            self.log.debug('Ignore check if its after 3pm to trade')
        
        # Initialize session: get up-to-date data, generate signals, get IB account details etc.
        self.start_session()

        # Check if signal data is up-to-date
        last_signals_ds = self._get_last_available_signal_ds()
        self.log.debug(f'Last available ds in signals is: {last_signals_ds}')
        if ignore_sa_check == False:
            if last_signals_ds != self.today:
                self.log.debug('Signals are not up to date. No trading')
                return None
        else:
            self.log.debug('Ignore check if signals have up to date processed data')
            self.log.debug(
                f"! Note that {last_signals_ds} will be used to get entry/exit signals, "
                f"but today is: {self.today} !"
            )

        # Gather buy/sell signals
        to_sell, buy_candidates = self._gather_buy_sell_signals(last_signals_ds)
        self.log.debug(f'Got exit signals from following symbols: {to_sell}')
        # Run position sizer to decide what to buy
        # NOTE: at this moment closing position orders are (most-probably) not yet filled
        # available money are not yet updated. so buying can be based on smaller amount here
        to_buy = self.position_sizer.decide_what_to_buy(
            self.available_cash,
            buy_candidates,
            volatility={
                c['symbol']: self.signals[c['symbol']]['volatility'][last_signals_ds] 
                for c in buy_candidates
            }
        )
        self.log.debug(f'Buying candidates: {buy_candidates}')

        # Place and monitor orders until everything is sold and bought
        while (len(to_sell) != 0) and (len(to_buy) != 0):
            to_buy, to_sell = self._place_and_monitor_sell_buy(to_buy, to_sell)
            # TMP: break here to avoid infinite loop till the point _place_and_monitor_sell_buy is correct
            break

        # Close trading for now
        self.log.debug(f'All thing are now sold and bough. Finish trading.')
        # That will be handled differently later on
        return None

        """
        High-level flow:
        - do things at the end of the session. e.g. 1-2h before closing
        - take data near end of the session as full data
        - this is closest thing to backtest scenario
        - risk of not filling orders... but it can be mitigated with limits etc.
        - flow would be:
            -> start at fixed hour of trading session (e.g. 1-2 before closing a session):
                - check all the things to sell
                - place sell orders
                - check all the things to buy
                - run position sizer to determine what to buy
                - place buy and stop loss orders (repeat until things are sold/bought)
            -> after session is closed:
                - cancel not filled buy orders (lost opportunity. happens.)
                - send an alert to me in case there is open sell order (any kind) that
                  should be filled but it's not

        # TODO: add here check if session is already closed

        # ? TODO: potentially _place_and_monitor_sell_buy can be wrapped in another 
        # loop so that _gather_buy_sell_signals is run multiple times



        Questions:
        1.
            What about money that I got from sell? Right now I just sequentially plan to send
            sell and buy order. That means I will have less money for buying things...

            After orders are out one should probablyt monitor order execution and continue to
            buy things as sell orders are filled..

        2.
            Closing positions should also be repeated probably... so that if along with time
            new symbol joins to_sell pool they are also included
        """

    def start_session(self):
        self.log.debug('Starting session!')
        self.universe = self._prepare_data(self.load_csv)
        self.log.debug('Prepared pricing data')
        self.signals = self._prepare_signals()
        self.log.debug('All signals ready')
        self.account_details = self.get_account_details()
        self.log.debug(
            'Initial account details ready: '
            f'available_cash: {self.available_cash}, hold_symbols: {self.hold_symbols}, '
            f'positions_cnts: {self.positions_cnts}',
        )

    def get_account_details(self):
        portfolio_details = self.ib_app.get_portfolio_details()
        self.available_cash = float(portfolio_details['TotalCashBalance_GBP'])
        self.hold_symbols = []
        self.positions_cnts = {}
        for symbol, details in portfolio_details['positions'].items():
            if (details['contractType'] == 'STK') and (details['positionCnt'] > 0):
                self.hold_symbols.append(symbol)
                self.positions_cnts[symbol] = int(portfolio_details['positions'][symbol]['positionCnt'])
        return portfolio_details

    def check_if_trade_open(self):
        """
        LSE is opened Mon.-Fri from 8am to 4:30 pm. There are couple of holidays though.
        """
        # Holidays for 2021
        if self.today == '2021-01-01':
            self.log.debug("Trading session is closed: New Year's Day  Friday, January 1, 2021")
            return False
        elif self.today == '2021-04-02':
            self.log.debug("Trading session is closed: Good Friday Friday, April 2, 2021")
            return False
        elif self.today == '2021-04-05':
            self.log.debug("Trading session is closed: Easter, Monday, April 5, 2021")
            return False
        elif self.today == '2021-05-03':
            self.log.debug("Trading session is closed: Bank Holiday, Monday, May 3, 2021")
            return False
        elif self.today == '2021-05-31':
            self.log.debug("Trading session is closed: Bank Holiday, Monday, May 31, 2021")
            return False
        elif self.today == '2021-12-24':
            self.log.debug("Trading session is closed: Christmas Friday, December 24, 2021")
            return False
        elif self.today == '2021-12-31':
            self.log.debug("Trading session is closed: New Year's Day, Friday, December 31, 2021")
            return False
        now = self._now()
        dow = now.weekday() # 0-Monday, 6-Sunday
        hour = now.hour
        if dow >= 5:
            self.log.debug("Trading session is closed: LSE is open Mon. - Fri. ")
            return False
        if hour < 8:
            self.log.debug("Trading session is closed: LSE is opens at 8am")
            return False
        if hour > 16 and now.minute > 30:
            self.log.debug("Trading session is closed: LSE is closes at 4:30pm")
            return False
        # session is open
        return True

    def _get_recent_not_today(self):
        """
        Get most recent session date that is NOT today
        """
        last_ds = '1990-01-01'
        prev_last_ds = '1990-01-01'
        for df in self.signals.values():
            _last_ds = str(df.index[-1])[:10]
            _prev_last_ds = str(df.index[-2])[:10]
            # find max last_ds
            if _last_ds > last_ds:
                last_ds = _last_ds
            # find max prev_last_ds:
            if _prev_last_ds > prev_last_ds:
                prev_last_ds = _prev_last_ds
        if last_ds == self.today:
            return prev_last_ds
        elif last_ds < self.today:
            return last_ds
        else:
            raise ValueError('last_ds cannot be bigger than today')

    def _get_last_available_signal_ds(self):
        last_ds = '1990-01-01'
        for df in self.signals.values():
            _last_ds = str(df.index[-1])[:10]
            if _last_ds > last_ds:
                last_ds = _last_ds
        return last_ds

    def _prepare_data(self, load_csv):
        lse_data = LSEData(pricing_data_path=self.pricing_data_path)
        symbols = lse_data.indicies_stocks['FTSE100']
        if load_csv == False:
            lse_data.download_data_to_csv(symbols=symbols)
        # NOTE: Lookback may impact consistency of generated signals. E.g. if for signal
        # one uses some sort of day-over-day calculations (e.g. OBV) that may cause
        # same date to have different signal depending on the run date 
        if self.signal_lookback != None:
            universe = helpers.get_recent_x_sessions(
                pricing_data=lse_data.load(symbols=symbols),
                days=self.signal_lookback,
            )
        else:
            universe = lse_data.load(symbols=symbols)
        translated_universe = {}
        for sym, df in universe.items():
            # calculate volatility if it does not exists. assuming 14 days rolling period 
            if 'volatility' not in df.columns:
                df.loc[:, 'volatility'] = df['close'].shift().rolling(14).std()
                df['volatility'].fillna(0, inplace=True)
            # translate FTSE symbols to its representation in IB (e.g. ADML == ADM)
            ib_symbol = ftse_100_to_ib_map[sym]
            translated_universe[ib_symbol] = universe[sym]
        return translated_universe

    def _prepare_signals(self):
        signals = {}
        for sym, df in self.universe.items():
            self.log.debug(f'Generating signal for: {sym}')
            self.universe[sym] = helpers.on_balance_volume_indicator(df)
            signals[sym] = SignalGenerator(
                df = self.universe[sym],
                config = self.signal_config
            ).generate()
        return signals

    def _gather_buy_sell_signals(self, ds):
        to_sell = []
        buy_candidates = []
        for symbol, df in self.universe.items():
            try:
                sym_cur_data = self.signals[symbol].loc[ds]
            except KeyError:
                # If signals are no up to date only for single symbol - ignore this one
                self.log.debug(f'No current data ({self.today}) for {symbol}. Assuming no entry/exit')
                continue
            # looking at `position` for selling is just precautious in case of missing exit trigger
            if (sym_cur_data['position'] == 0) or (sym_cur_data['exit_long'] == 1):
                to_sell.append(symbol)
            elif sym_cur_data['entry_long'] == 1:
                # note: close is not actual close here. its latest price from investpy dowloaded data
                approx_close = sym_cur_data['close']
                # this stop loss will be used only in position sizer. not as an input into order
                approx_sl = approx_close - (approx_close * self.stop_loss_perc)
                buy_candidates.append(
                    self.position_sizer.define_candidate(
                        symbol=symbol, entry_type='long', price=approx_close, stop_loss=approx_sl
                    )
                )
        return to_sell, buy_candidates

    def _execute_exit_signals(self, to_sell):
        held_for_sell = [
            symbol for symbol in to_sell if symbol in self.hold_symbols
        ]
        self.log.debug(f'Those are held symbols that should be sold: {held_for_sell}')
        for symbol in held_for_sell:
            contract = self.ib_app.get_contract(symbol=symbol)
            # it will be Adaptive Market Order with Normal priority
            shares_cnt = self.positions_cnts[symbol]
            order = self.ib_app.create_order(
                action='SELL',
                quantity=shares_cnt,
                orderType='MKT',
                adaptive=True
            )
            self.ib_app.placeOrder(self.ib_app.nextOrderId(), contract, order)
            self.log.debug(f'Placed SELL order for {shares_cnt} shares of {symbol}')

    def _execute_entry_signals(self, to_buy):
        for candidate in to_buy:
            symbol = candidate['symbol']
            # You cannot make BUY order for symbols you already own 
            # BUG! What if symbol is not hold yet but there is open order for it?
            if symbol not in self.hold_symbols:
                contract = self.ib_app.get_contract(symbol=symbol)
                shares_cnt = candidate['shares_count']
                # Adaptive Limit Order with Normal priority
                buy_order = self.ib_app.create_order(
                    action='BUY',
                    quantity=shares_cnt,
                    orderType='MKT',
                    adaptive=True
                )
                self.ib_app.placeOrder(self.ib_app.nextOrderId(), contract, buy_order)
                self.log.debug(f'Placed BUY order for {shares_cnt} shares of {symbol}')
                # protective TRAILING STOP LOSS sell order
                _stop_loss = int(self.stop_loss_perc*100)
                sell_order = self.ib_app.create_order(
                    action='SELL',
                    quantity=shares_cnt,
                    orderType='TRAIL',
                    trailingPercent=_stop_loss,
                )
                self.ib_app.placeOrder(self.ib_app.nextOrderId(), contract, sell_order)
                self.log.debug(
                    f'Placed protective sell Stop Loss ({_stop_loss}%) order for ' 
                    f'{shares_cnt} shares of {symbol}'
                )

    def _place_and_monitor_sell_buy(self, to_buy, to_sell):
        """
        This is not yet done. There are bugs here... It needs to be fixed.
        Probably a good way to do this will be checking get_current_orders status
        to get information about hanging orders...

        But its not that obvious as I want to wait for sell execution...

        Potentially, one option is to look at opened orders in _execute_{exit|entry}_signals.
        """
        # Place sell orders for held symbols that should be closed
        ### BUG!!!
        # This is now in loop. So if SELL order are not filled, updated to_sell will
        # be exactly the same, and at this point you will send the same closing orders...
        self._execute_exit_signals(to_sell)

        # Place BUY orders and simultaneous protective TRAILING STOP LOSS orders
        ### BUG!!!
        # Same problem as above - if orders are still hanging, orders will be repeated
        self.log.debug(f'To buy based on position sizing: {to_buy}')
        self._execute_entry_signals(to_buy)

        # Give time for trading execution, update portfolio status, place subsequent orders
        self.log.debug('Sleep for 1min to give placed (if any) orders some time')
        time.sleep(60) 
        
        # Update account details
        self.account_details = self.get_account_details()
        self.log.debug(
            'Updated account details: '
            f'available_cash: {self.available_cash}, hold_symbols: {self.hold_symbols}, '
            f'positions_cnts: {self.positions_cnts}',
        )
        # Compare hold positions to those which supposed to be sold. Update to_sell
        # (New to_sell should be an intersection of held and to_sell)
        to_sell = list(set(self.hold_symbols).intersection(set(to_sell)))
        self.log.debug(f'Remaining symbols to sell: {to_sell}')

        # If there were things sold, then it is possible that you have more money to buy
        # Look again at candidates (exclude to_buy for which there were already orders sent)
        more_buy_candidates = [
            c for c in buy_candidates if c['symbol'] not in [tb['symbol'] for tb in to_buy]
        ]
        more_to_buy = self.position_sizer.decide_what_to_buy(
            self.available_cash,
            more_buy_candidates,
            volatility={
                c['symbol']: self.signals[c['symbol']]['volatility'][last_signals_ds] 
                for c in more_buy_candidates
            }
        )
        # Update list of shares to buy to include new symbols and exclude those already bought
        to_buy = to_buy.extend(more_to_buy)
        to_buy = [tb for tb in to_buy if tb['symbol'] not in self.hold_symbols]
        return to_buy, to_sell

    def _check_ib_env(self):
        # TODO: Implement. This should be additional check for test/prod account 
        pass

    def _now(self):
        return datetime.datetime.now(pytz.timezone('Europe/London'))


def main(download_data=True, ib_port=None, debug=False, ignore_xe_check=False, 
         ignore_tod_check=False, ignore_sa_check=False, lookback=None):
    position_sizer = FixedRisk(
        fee_perc = 0,
        min_fee = 6,
        sort_type = 'rrr',
        risk_per_trade = 80,
    )
    executor = TradingExecutor(
        pricing_data_path='/Users/slaw/osobiste/trading/pricing_data',
        load_csv=download_data,
        position_sizer=position_sizer,
        signal_config=long_only_s4_config,
        signal_lookback=lookback,
        ib_port=ib_port,
        debug=debug,
    )
    executor.trade(
        ignore_xe_check=ignore_xe_check,
        ignore_tod_check=ignore_tod_check,
        ignore_sa_check=ignore_sa_check,
    )


if __name__ == '__main__':
    parser = commons.get_parser()
    parser.add_argument(
        '--skip_download', '-sd',
        action='store_true',
        help='flag to skip downloading data. will read from csv',
    )
    parser.add_argument(
        '--prod_port', '-pp',
        action='store_const',
        const=1234,  # not valid port yet
        help='production port that IB App should connect to'
    )
    parser.add_argument(
        '--lookback', '-lb',
        action='store',
        default=None,
        type=int,
    )
    # add agrs for trading checks
    parser.add_argument(
        '--ignore_xe_check', '-iex',
        action='store_true',
        help='flag to skip check if exchange is open',
    )
    parser.add_argument(
        '--ignore_tod_check', '-itd',
        action='store_true',
        help='flag to skip time of day check',
    )
    parser.add_argument(
        '--ignore_sa_check', '-isa',
        action='store_true',
        help='flag to skip signals ds availability check',
    )
    args = parser.parse_known_args()[0]
    print(f'Script args: {args}')
    if args.prod_port == None:
        # Paper Trading port
        port = 7497
    else:
        port = args.prod_port
    main(
        debug=args.debug,
        download_data=args.skip_download,
        lookback=args.lookback,
        ib_port=port,
        ignore_xe_check=args.ignore_xe_check,
        ignore_tod_check=args.ignore_tod_check,
        ignore_sa_check=args.ignore_sa_check,
    )


"""
example signals[symbol] dataframe

open   high     low  close     volume         obv  entry_long  exit_long  entry_short  exit_short  position
date                                                                                                                    
2020-08-06  608.6  614.8  595.80  610.0  1684018.0  -1684018.0         0.0        0.0          0.0         0.0         0
2020-08-07  607.6  616.8  602.00  611.8  1382306.0   -301712.0         0.0        0.0          0.0         0.0         0
2020-08-10  618.6  630.0  612.80  620.6  2190978.0   1889266.0         0.0        0.0          0.0         0.0         0
2020-08-11  626.2  646.0  626.00  644.2  2867888.0   4757154.0         0.0        0.0          0.0         0.0         0
2020-08-12  642.8  657.4  638.36  644.8  2556613.0   7313767.0         0.0        0.0          0.0         0.0         0
...           ...    ...     ...    ...        ...         ...         ...        ...          ...         ...       ...
2020-12-18  821.2  826.2  811.60  814.0  4888155.0  36496095.0         0.0        0.0          0.0         0.0         1
2020-12-21  794.0  796.0  772.20  787.4  3221088.0  33275007.0         0.0        0.0          0.0         0.0         1
2020-12-22  784.6  799.0  778.20  796.6  1766023.0  35041030.0         0.0        0.0          0.0         0.0         1
2020-12-23  794.0  813.8  791.80  812.4  1273175.0  36314205.0         0.0        0.0          0.0         0.0         1
2020-12-24  812.4  819.8  805.60  813.0   675017.0  36989222.0         0.0        0.0          0.0         0.0         1
"""
