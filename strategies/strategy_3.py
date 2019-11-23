"""
Strategy 3 - Simple strategy made based on "Filter rule". It is made to additionally test my first iteration
of singal generation framework. 

How th rule works:
This rule is based on filters created on the top of support/resistance. If price exeedes support/ressistance by b% then there is correspoding signal.
There are 2 versions of strategy. Binary - with 1 filter and being long/short all the time. 2nd version requires additional filter and allows
neutral position.
"""

# built-in
import copy
import sys
import itertools
sys.path.insert(0, '/Users/slaw/osobiste/trading')

# custom
import backtester
import commons
import gpw_data
import position_size
import rules
import signal_generator
import strategy


CONFIG = {
    'rules': [
        {
            'id': 'filter_1',
            'type': 'simple',
            'ts': 'close',
            'lookback': 56,
            'params': {'b': 0.02},
            'func': rules.support_resistance,
        },
        {
            'id': 'filter_2',
            'type': 'simple',
            'ts': 'close',
            'lookback': 7,
            'params': {'b': 0.015},
            'func': rules.support_resistance,
        },
        {
            'id': 'filter_rule_extended',
            'type': 'convoluted',
            'simple_rules': ['filter_1', 'filter_2'],
            'aggregation_type': 'state-based',
            'aggregation_params': {
                'long': [
                    {'filter_1': 1, 'filter_2': 1}
                ],
                'short': [
                    {'filter_1': -1, 'filter_2': -1}
                ],
                'neutral': [
                    {'filter_1': 0, 'filter_2': 0},
                ]
            }
        },
        {
            'id': 'filter_rule_binary',
            'type': 'convoluted',
            'simple_rules': ['filter_1'],
            'aggregation_type': 'state-based',
            'aggregation_params': {
                'long': [{'filter_1': 1}],
                'short': [{'filter_1': -1}],
            }
        }
    ],
    'strategy': {
        'type': 'fixed',
        'strategy_rules': ['filter_rule_binary'],
        'constraints': {
            'hold_x_days': 28,
            'wait_entry_confirmation': 7
        }
    }
}


def create_config(f1_lookback=None, f1_b=None, f2_lookback=None, f2_b=None, hold_x_days=None, wait=None, binary=None):
    new_config = copy.deepcopy(CONFIG)
    if f1_lookback:
        new_config['rules'][0]['lookback'] = f1_lookback
    if f1_b:
        new_config['rules'][0]['params']['b'] = f1_b
    if f2_lookback:
        new_config['rules'][1]['lookback'] = f2_lookback
    if f2_b:
        new_config['rules'][1]['params']['b'] = f2_b
    if isinstance(hold_x_days, int):
        new_config['strategy']['constraints']['hold_x_days'] = hold_x_days
    else:
        new_config['strategy']['constraints'].drop('hold_x_days')
    if isinstance(wait, int):
        new_config['strategy']['constraints']['wait_entry_confirmation'] = wait
    else:
        new_config['strategy']['constraints'].drop(['wait_entry_confirmation'])
    if binary:
        new_config['strategy']['strategy_rules'] = ['filter_rule_binary']
        new_config['rules'].pop(1) # delete rule id = filter_2, list indecies will change
        new_config['rules'].pop(1) # delete rule id = filter_rule_extended
    return new_config


def generate_signal(df, f1_lookback=None, f1_b=None, f2_lookback=None, f2_b=None, hold_x_days=None, wait=None, binary=None):
    if f2_b > f1_b:
        raise ValueError('f1_b has to be bigger than f2_b')
    sg = signal_generator.SignalGenerator(
        df = df,
        config = create_config(
            f1_lookback=f1_lookback,
            f1_b=f1_b,
            f2_lookback=f2_lookback,
            f2_b=f2_b,
            hold_x_days=hold_x_days,
            wait=wait,
            binary=binary
        ),
    )
    return sg.generate()


def optimize():
    data_collector = gpw_data.GPWData()
    symbol = 'CCC'
    pricing_data = {
        symbol: data_collector.load(symbols=symbol, from_csv=True)
    }
    data_test, data_validation = data_collector.split_into_subsets(pricing_data, 0.5)
    position_sizer = position_size.MaxFirstEncountered()

    strategy_kwargs = {
        'f1_lookback': [7, 14, 28, 56],
        'f1_b': [
            0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.06, 
            0.07, 0.08, 0.09, 0.1, 0.12, 0.14, 0.16, 0.18, 0.2, 0.25, 0.3, 0.4, 0.5
        ],
        'f2_lookback': [7, 14, 28, 56],
        'f2_b': [0.005, 0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05, 0.075, 0.1, 0.15, 0.2],
        'hold_x_days': [1, 2, 5, 10, 16, 28],
        'wait': [1, 2, 3, 7],
        'binary': [True, None],
    }

    print('Start optimization')
    res = strategy.optimize_strategy(
        data=data_test,
        signal_gen_func=generate_signal,
        strategy_kwargs=strategy_kwargs,
        position_sizer=position_sizer,
        init_capital=10000,
        results_path='/Users/slaw/osobiste/trading/filter_rule_opt_results_all.csv',
    )


def run_strategy(days=-1, debug=False):
    data_collector = gpw_data.GPWData()
    symbol = 'CCC'
    pricing_data = {
        symbol: data_collector.load(symbols=symbol, from_csv=True)
    }
    position_sizer = position_size.MaxFirstEncountered()

    sg = signal_generator.SignalGenerator(
        df = pricing_data[symbol],
        config = CONFIG,
    )

    tester = backtester.Backtester(
        {symbol: sg.generate()},
        position_sizer=position_sizer,
        debug=debug,
    )

    if days == -1:
        tester_results, tester_trades = tester.run()
    else:
        tester_results, tester_trades = tester.run(test_days=days)

    print(tester_results[-20:])
    print('... Done! [OK]')


if __name__ == '__main__':
    parser = commons.get_parser()
    parser.add_argument('--days', '-d', type=int, default=-1, help='number of days to run backtester for')
    parser.add_argument('--optimize', '-o', action='store_true', help='run optimization')
    args = parser.parse_args()
    if args.optimize:
        optimize()
    else:
        run_strategy(args.days, args.debug)

    # TODO-1 -> fully test filter rule (backtester+optimization)
    # TODO-2 -> implement moving averages rule
    # TODO-3 -> implement learning strategies (there are 2 kinds)
    # TODO-4 -> implement result calculation (same as from the book. need all the bootstrap/monte carlo tests for data-mining)

