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
        - in GBX (1/100 of GBP)
        - predefined universe
        - LSE and trading hours
        - fixed stop loss %
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
        self.stop_loss_perc = stop_loss_perc/100.0  # `stop_loss_perc` is in %, i.e. 1% -> 1
        self.position_sizer = position_sizer
        self._to_set_stop_loss = []
        # start IB App
        self.ib_app = IBAPIApp(
            port=ib_port,
            clientId=ib_client,
        )
        self._check_ib_env()

    def trade(self, ignore_xe_check=False, ignore_tod_check=False, ignore_sa_check=False):
        """
        High-level flow:
        - Run checks:
            - is exchange open? 
            - what time is it? - it should start execution ~1-2h before closing
        - Initialize session:
            - download/get pricing data
            - run signal generation
            - get current account status: money and what you currently hold
        - More checks:
            - are generated signals up to date?
        - Gather entry/exit signals -> lists of things to buy and sell
        - Place and monitor orders until everything is sold and bought
            - see _place_and_monitor_sell_buy for more details
        - Exit
            - This will be changed later.
        - Generates signals again with more up to date data from investpy?
            - This is open question. Not sure if it makes sense right now
        - Do things after session is closed
            - Set protective stop loss orders
            - [P3] Cancel not filled buy orders
        """
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
        self.log.debug(f'Buying candidates: {buy_candidates}')
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
        self.log.debug(f'Symbols/shares to buy based on position sizing: {to_buy}')

        # Place and monitor orders until everything is sold and bought
        while (len(to_sell) != 0) or (len(to_buy) != 0):
            self.log.debug(
                f'Enter `_place_and_monitor_sell_buy` with to_buy: {to_buy} and to_sell: {to_sell}'
            )
            to_buy, to_sell = self._place_and_monitor_sell_buy(
                to_buy, to_sell, buy_candidates, last_signals_ds
            )

        # Wait until session is closed to place protective orders at a close price
        while (self._now().hour < 16) and (self._now().minute < 30):
            time.sleep(60*5)
            self.log.debug(f'Waiting for session to close (16:30). Now is: {self._now()}')
        self._place_protective_orders()

        # Close trading
        self.log.debug(f'All thing are now sold and bough - trading finished.')
        return

    def start_session(self):
        self.log.debug('Starting session!')
        self.universe = self._prepare_data(self.load_csv)
        self.log.debug('Prepared pricing data')
        self.signals = self._prepare_signals()
        self.log.debug('All signals ready')
        self.account_details = self.get_account_details()

    def get_account_details(self):
        portfolio_details = self.ib_app.get_portfolio_details()
        # NOTE: available_cash is converted into GBX, that is 1/100 GBP
        self.available_cash = float(portfolio_details['TotalCashBalance_GBP']) * 100
        self.hold_symbols = []
        self.positions_cnts = {}
        for symbol, details in portfolio_details['positions'].items():
            if (details['contractType'] == 'STK') and (details['positionCnt'] > 0):
                self.hold_symbols.append(symbol)
                self.positions_cnts[symbol] = int(portfolio_details['positions'][symbol]['positionCnt'])
        self.log.debug(
            'Account details ready: '
            f'available_cash: {self.available_cash}, hold_symbols: {self.hold_symbols}, '
            f'positions_cnts: {self.positions_cnts}',
        )
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
        if hour >= 16 and now.minute > 30:
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

    def _execute_exit_signals(self, to_sell, to_cancel):
        held_for_sell = [
            symbol for symbol in to_sell if symbol in self.hold_symbols
        ]
        self.log.debug(f'Will close positions for following symbols: {held_for_sell}')
        for symbol in held_for_sell:
            contract = self.ib_app.get_contract(symbol=symbol)
            # it will be Adaptive Market Order with Normal priority
            shares_cnt = self.positions_cnts[symbol]
            order = self.ib_app.create_order(
                action='SELL',
                quantity=shares_cnt,
                orderType='MKT',
                adaptive=True,
                tif='DAY'
            )
            self.ib_app.placeOrder(self.ib_app.nextOrderId(), contract, order)
            self.log.debug(f'Placed SELL order for {shares_cnt} shares of {symbol}')
        self.log.debug(f'Will cancel following protective orders: {list(to_cancel.items())}')
        for orderId, symbol in to_cancel.items():
            self.ib_app.cancelOrder(orderId)
            self.log.debug(f'Cancelled {shares_cnt} ({symbol})')

    def _execute_entry_signals(self, to_buy):
        self.log.debug(f"Will open positions for following symbols: {[tb['symbol'] for tb in to_buy]}")
        for candidate in to_buy:
            symbol = candidate['symbol']
            if symbol in self.hold_symbols:
                self.log.debug((
                    f"Skipping placing order for {symbol} as it is already bought. "
                    'You cannot place BUY order for symbols you already own'
                ))
                continue
            contract = self.ib_app.get_contract(symbol=symbol)
            shares_cnt = candidate['shares_count']
            # Adaptive Limit Order with Normal priority
            buy_order = self.ib_app.create_order(
                action='BUY',
                quantity=shares_cnt,
                orderType='MKT',
                adaptive=True,
                tif='DAY'
            )
            self.ib_app.placeOrder(self.ib_app.nextOrderId(), contract, buy_order)
            self.log.debug(f'Placed BUY order for {shares_cnt} shares of {symbol}')
            self._to_set_stop_loss.append({
                'symbol': symbol, 'cnt': shares_cnt
            })

    def _place_protective_orders(self):
        self.log.debug(
            f"Stop Loss orders execution: {[s['symbol'] for s in self._to_set_stop_loss]}"
        )
        _stop_loss = self.stop_loss_perc*100
        for s in self._to_set_stop_loss:
            contract = self.ib_app.get_contract(symbol=s['symbol'])
            sell_order = self.ib_app.create_order(
                action='SELL',
                quantity=s['shares_cnt'],
                orderType='TRAIL',
                trailingPercent=_stop_loss,
            )
            self.ib_app.placeOrder(self.ib_app.nextOrderId(), contract, sell_order)
            self.log.debug(
                f"Placed protective sell Stop Loss ({_stop_loss}%) order for "
                f"{s['shares_cnt']} shares of {s['symbol']}"
            )

    def _place_and_monitor_sell_buy(self, to_buy, to_sell, buy_candidates, last_signals_ds):
        """
        Input:
            to_buy: filtered list of candidates to buy (check struct in position sizer)
            to_sell: list of symbols
            buy_candidates: list of all candidates to buy
            last_signals_ds: YYYY-MM-DD str indicating ds to use
        
        High-level flow:
            - This method is executed in loop until to_buy and to_sell are empty
            - At first it takes currently opened orders to filter out symbols from to_buy/sell
              so that same orders are not placed twice in case order is still not filled
            - Then it closes positions from to_sell (send SELL ordedrs)
            - Then opens positions from to_buy (BUY orders + protective SELL orders with Stop Loss)
            - Waits a while for orders to fill (those are market orders so expecting relatively
              fast fill time)
            - After that, update account details so that money and positions are up to date
            - Then is defines more things to buy. There should be more money comming from sold
              things. This can be used to buy more.
            - At the end, to_buy/to_sell should be up to date so that next iteration is ok 
        """
        # Get currently opened orders
        opened_orders = self.ib_app.get_current_orders()
        while opened_orders == None:
            self.log.debug('Could not get opened orderes. Wait 1min and trying again.')
            time.sleep(60)
            opened_orders = self.ib_app.get_current_orders()
        
        # Check all the current orders and update to_buy/to_sell lists accordingly
        # [{'symbol': s1, ...}, ...] -> {s1: 0, ...}
        to_buy_sym_idx_map = {s: idx for idx, s in enumerate([tb['symbol'] for tb in to_buy])}
        # [s1, s2, ...] -> {s1: 0, s2: 1, ...}
        to_sell_sym_idx_map = {s: idx for idx, s in enumerate(to_sell)}
        _opened_buy_symbols = []
        _protective_to_cancel = {}
        # (if there are opened orders already -> apporiatly remove elements from lists)
        for order_id in opened_orders['orders']:
            _order = opened_orders[order_id]
            _symbol = _order['symbol']
            # as all order statuses (e.g. Submitted or Filled) are relevant - there is no check
            if _order['action'] == 'BUY':
                if _symbol in to_buy_sym_idx_map.keys():
                    to_buy.pop(to_buy_sym_idx_map[_symbol])
                    _opened_buy_symbols.append(_symbol)
            elif _order['action'] == 'SELL':
                # check if symbol is on the to sell list and its not protective SL order
                if (_symbol in to_sell) and (_order['orderType'] == 'MKT'):
                    to_sell.pop(to_sell_sym_idx_map[_symbol])
                # get protective SL orders that should be cancelled while selling
                elif (_symbol in to_sell) and (_order['orderType'] == 'TRAIL'):
                    _protective_to_cancel[order_id] = _symbol

        self.log.debug(
            f"There are orders opened for following symbols right now: {_opened_buy_symbols}"
        )
        # At this point to_buy/to_sell lists are aware about currently placed orders

        # Place sell orders for held symbols that should be closed
        self._execute_exit_signals(to_sell, _protective_to_cancel)

        # Place BUY orders and simultaneous protective TRAILING STOP LOSS orders
        self._execute_entry_signals(to_buy)

        # Give time for trading execution, update portfolio status, place subsequent orders
        self.log.debug('Sleep for 2mins to give placed (if any) orders some time')
        time.sleep(120)
        
        # Update account details (this will make available_cash and hold_symbols up-to-date)
        self.account_details = self.get_account_details()

        # Compare hold positions to those which supposed to be sold. Update to_sell
        # (New to_sell should be an intersection of held and to_sell)
        to_sell = list(set(self.hold_symbols).intersection(set(to_sell)))
        self.log.debug(f'Remaining symbols to sell: {to_sell}')

        # If there were things sold, then it is possible that you have more money to buy
        # Look again at candidates (exclude to_buy for which there were already orders sent)
        more_buy_candidates = [
            c for c in buy_candidates
            if c['symbol'] not in ([tb['symbol'] for tb in to_buy] + _opened_buy_symbols)
        ]
        self.log.debug(f'More possible candidates to BUY: {more_buy_candidates}')
        more_to_buy = self.position_sizer.decide_what_to_buy(
            self.available_cash,
            more_buy_candidates,
            volatility={
                c['symbol']: self.signals[c['symbol']]['volatility'][last_signals_ds] 
                for c in more_buy_candidates
            }
        )
        self.log.debug(f'Adding {more_to_buy} to the list of things to BUY')
        # Update list of shares to buy to include new symbols and exclude those already bought
        to_buy.extend(more_to_buy)
        to_buy = [tb for tb in to_buy if tb['symbol'] not in self.hold_symbols]
        self.log.debug(f'End of execution cycle: to_buy: {to_buy}, to_sell: {to_sell}')
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
        min_fee = 6*100,
        sort_type = 'rrr',
        risk_per_trade = 125*100,
        debug=debug,
        allow_partial=True,
    )
    executor = TradingExecutor(
        pricing_data_path='/Users/slaw/osobiste/trading/pricing_data',
        load_csv=download_data,
        position_sizer=position_sizer,
        signal_config=long_only_s4_config,
        signal_lookback=lookback,
        ib_port=ib_port,
        debug=debug,
        stop_loss_perc=1.5,
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
