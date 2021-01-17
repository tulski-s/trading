"""
Strategy 4 - Based on some good performing rule on data mining experiment. Uses proper position sizing and automatic stop loss 
"""

# built in
import sys
sys.path.insert(0, '/Users/slaw/osobiste/trading')
sys.path.insert(0, '/Users/slaw/osobiste/trading/strategies')

import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('/Users/slaw/osobiste/trading/strategies/str4.log')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


# custom
from lse_data import LSEData
from backtester import Backtester
from signal_generator import SignalGenerator
from position_size import FixedRisk

import results
import helpers
import rules


long_only_s4_config = {
    'rules': [
        {
            'id': 'oba_s_n2',
            'type': 'simple',
            'ts': 'obv',
            'lookback': 7,
            'params': {},
            'func': rules.moving_average
        },
        {
            'id': 'filter_07_lb2',
            'type': 'simple',
            'ts': 'close',
            'lookback': 2,
            'params': {
                'b': 0.07,
            },
            'func': rules.support_resistance
        },
        {
            'id': 'rev_filter_5_lb7',
            'type': 'simple',
            'ts': 'close',
            'lookback': 7,
            'params': {
                'b': 0.05,
                'e': 2,
            },
            'func': rules.support_resistance
        },
        {
            'id': 'ma_S_n25m20',
            'type': 'simple',
            'ts': 'close',
            'lookback': 25,
            'params': {
                'quick_ma_lookback': 20,
            },
            'func': rules.moving_average
        },
        {
            'id': 'long_only_oba_s_n2',
            'type': 'convoluted',
            'simple_rules': ['oba_s_n2'],
            'aggregation_type': 'state-based',
            'aggregation_params': {
                'long': [
                    {'oba_s_n2': 1}
                ],
                'neutral': [
                    {'oba_s_n2': 0},
                ]
            }
        },
        {
            'id': 'long_only_filter_07_lb2',
            'type': 'convoluted',
            'simple_rules': ['filter_07_lb2'],
            'aggregation_type': 'state-based',
            'aggregation_params': {
                'long': [
                    {'filter_07_lb2': 1}
                ],
                'neutral': [
                    {'filter_07_lb2': 0},
                ]
            }
        },
        {
            'id': 'long_only_rev_filter_5_lb7',
            'type': 'convoluted',
            'simple_rules': ['rev_filter_5_lb7'],
            'aggregation_type': 'state-based',
            'aggregation_params': {
                'long': [
                    {'rev_filter_5_lb7': -1}
                ],
                'neutral': [
                    {'rev_filter_5_lb7': 0},
                    {'rev_filter_5_lb7': 1}
                ]
            }
        },
        {
            'id': 'long_only_ma_S_n25m20',
            'type': 'convoluted',
            'simple_rules': ['ma_S_n25m20'],
            'aggregation_type': 'state-based',
            'aggregation_params': {
                'long': [
                    {'ma_S_n25m20': 1}
                ],
                'neutral': [
                    {'ma_S_n25m20': 0},
                ]
            }
        },
    ],
    'strategy': {
        'type': 'learning',
        'strategy_rules': [
            'long_only_oba_s_n2',
            'long_only_filter_07_lb2',
            'long_only_rev_filter_5_lb7',
            'long_only_ma_S_n25m20',
        ],
        'params':{
            'memory_span': 20,
            'review_span': 10,
            'performance_metric': 'daily_returns',
            'price_label': 'close',
        },
        'strategy_id': 'long_only_oba_s_n2',
    }
}


def main():
    # get and prepare data
    lse_data = LSEData(pricing_data_path='/Users/slaw/osobiste/trading/pricing_data')
    universe = lse_data.load(
        symbols=lse_data.indicies_stocks['FTSE100']
    )
    print(f'No. of stocks in universe: {len(universe.keys())}')
    for sym, df in universe.items():
        universe[sym] = helpers.on_balance_volume_indicator(df)

    signals = {}
    for sym, df in universe.items():
        print('Processing: ', sym)
        signals[sym] = SignalGenerator(
            df = universe[sym],
            config = long_only_s4_config
        ).generate()

    print('All symbols generated. Moving to backtest')

    print('Splitting data into train/test')
    train_data, test_data = helpers.split_into_subsets(signals, 0.5)

    # set up (scale by 100 as prices are in GBX that is 1/100 of GBP)
    init_capital = 10000*100
    risk_per_trade = 125*100
    auto_stop_loss = 0.015
    volatility_lb = 14
    position_sizer = FixedRisk(
        fee_perc = 0,
        min_fee = 6*100,
        sort_type = 'rrr',
        risk_per_trade = risk_per_trade,
        #allow_partial=True,
    )
    test_backtester = Backtester(
        test_data,
        init_capital=init_capital,
        position_sizer=position_sizer,
        auto_stop_loss=auto_stop_loss,
        volatility_lb=volatility_lb,
        logger=logger,
        debug=True,
    )

    print('Starting backtest')
    test_results_df, test_trades = test_backtester.run()
    results.performance_report(test_results_df, test_trades)

    #print(test_trades)
    print(test_results_df.tail(100))

    print('Done!')


if __name__ == '__main__':
    main()