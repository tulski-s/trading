# built-in
import datetime
import sys

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


class TradingExecutor():
    """
    As it is right now it is NOT some sort of generic executor. Right now it is built for and supports
    very specific trading strategy (long only, stocks only, in GBP, predefined universe, etc.)
    """
    def __init__(self, pricing_data_path='./pricing_data', load_csv=False, logger=None, debug=False, 
                 signal_config=None, signal_lookback=None, ib_port=None, ib_client=666):
        self.ds = datetime.datetime.now().strftime('%Y-%m-%d')
        self.log = commons.setup_logging(logger=logger, debug=debug)
        self.pricing_data_path = pricing_data_path
        self.load_csv = load_csv
        self.signal_config = signal_config
        self.signal_lookback = signal_lookback
        # start IB App
        self.ib_app = IBAPIApp(
            port=ib_port,
            clientId=ib_client,
        )
        self._check_ib_env()

    def trade(self):
        # initialize session. get data, generate signal, get IB account details etc.
        self.start_session()

        recent_not_today = self._get_recent_not_today()
        self.log.debug(f'Recent not today date is: {recent_not_today}')

        # check what you own vs. what signals indicate you should own
        # determine things that still should be sold
        held_for_sell = []
        for sym in self.hold_symbols:
            _df = self.signals.get(sym, None)
            if isinstance(_df, type(None)):
                # ignore symbols that you hold but are not in the universe
                continue
            _day_data = _df.loc[recent_not_today]
            # no long position or long exit signal
            if (_day_data['position'] == 0) or (_day_data['exit_long'] == 1):
                held_for_sell.append(sym)
        self.log.debug(f'Held symbols that should be sold: {held_for_sell}')
        

        """
        High-level flow:
        - check what you own from the things you should be on given position
        - check if there is anything to exit which you still own (should be rare as I'd sell at the ~end of previous session)
        - check what should be entered
            -> entry signals for given day
            -> get current day (live) data
            -> assign appropriate stop loss
            -> run position sizer
        """

    def start_session(self):
        print('Started session!')
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
        self.available_cash = portfolio_details['TotalCashBalance_GBP']
        self.hold_symbols = []
        self.positions_cnts = {}
        for symbol, details in portfolio_details['positions'].items():
            if details['contractType'] == 'STK':
                self.hold_symbols.append(symbol)
                self.positions_cnts[symbol] = portfolio_details['positions'][symbol]['positionCnt']
        return portfolio_details

    def _get_recent_not_today(self):
        """
        Get most recent session date that is NOT today
        """
        today = str(datetime.datetime.now(pytz.timezone('Europe/London')).date())
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
        if last_ds == today:
            return prev_last_ds
        elif last_ds < today:
            return last_ds
        else:
            raise ValueError('last_ds cannot be bigger than today')

    def _prepare_data(self, load_csv):
        lse_data = LSEData(pricing_data_path=self.pricing_data_path)
        symbols = lse_data.indicies_stocks['FTSE100']
        if load_csv == False:
            lse_data.download_data_to_csv(symbols=symbols)
        universe = helpers.get_recent_x_sessions(
            pricing_data=lse_data.load(symbols=symbols),
            days=self.signal_lookback,
            ignore_current_ds=True,
        )
        # translate FTSE symbols to its representation in IB (e.g. ADML == ADM)
        translated_universe = {}
        for sym in universe.keys():
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

    def _check_ib_env(self):
        # TODO: Implement. This should be additional check for test/prod account 
        pass


def main(download_data=True, ib_port=None, debug=False):
    executor = TradingExecutor(
        pricing_data_path='/Users/slaw/osobiste/trading/pricing_data',
        load_csv=download_data,
        signal_config=long_only_s4_config,
        signal_lookback=100,
        ib_port=ib_port,
        debug=debug,
    )
    executor.trade()


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
        ib_port=port,
    )


"""
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