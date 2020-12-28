# built-in
import datetime
import sys

sys.path.insert(0, '/Users/slaw/osobiste/trading')

# custom
import commons
from lse_data import LSEData
from signal_generator import SignalGenerator
from strategies.strategy_4 import long_only_s4_config
import strategies.helpers as helpers
from ib_api import IBAPIApp


class TradingExecutor():
    """
    As it is right now it is NOT some sort of generic executor. Right now it is built for and supports
    very specific trading strategy (long only, stocks only, in GBP, etc.)
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
        self.start_session()
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

    def _prepare_data(self, load_csv):
        lse_data = LSEData(pricing_data_path=self.pricing_data_path)
        symbols = lse_data.indicies_stocks['FTSE100']
        if load_csv == False:
            lse_data.download_data_to_csv(symbols=symbols)
        universe = lse_data.load(symbols=symbols)
        return helpers.get_recent_x_sessions(
            pricing_data=universe,
            days=self.signal_lookback,
            ignore_current_ds=True,
        )

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
