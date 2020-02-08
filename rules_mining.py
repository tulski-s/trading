#built-in
import random

# custom
import backtester

# import commons
import gpw_data
import position_size
import results
import rules
import signal_generator


def wrc_sampling_dist(rules_results, daily_ret_col='daily_returns', no_samples=5000):
    """
    Creates sampling distribution for White's Reality Check (bootrap method for many trading rules to avoid data-mining bias)
    Input: dict with rule_names and DataFrame with daily returns column present
    """
    lengths = [df.shape[0] for df in rules_results.values()]
    try:
        assert(len(set(lengths)) == 1)
    except AssertionError:
        raise AssertionError(
            """
            Not all rules have the same number of results. For method to work properly, each rule should have exactly 
            the same dates (number of daily returns). Results legnts: {}
            """.format(lengths)
        )
    for df in rules_results.values():
        # shift daily returns so that each rule avg. return is equal to 0
        mean_ret = df[daily_ret_col].mean()
        df.loc[:, 'shifted_daily_returns'] = df[daily_ret_col] - mean_ret
        df.fillna(value={f'shifted_daily_returns':0}, inplace=True)
    sample_size = lengths[0]
    max_avg_rets = []
    for k in range(no_samples):
        sample_idxs = list(range(sample_size))
        random_idxs = [random.choice(sample_idxs) for _ in sample_idxs]
        avg_rets = []
        for df in rules_results.values():
            avg_rets.append(
                df['shifted_daily_returns'].iloc[random_idxs].mean()
            )
        max_avg_rets.append(max(avg_rets))
    return max_avg_rets


def main():
    # defines rules to test
    single_rules = {
        'filter_1': {
            'id': 'filter_1',
            'type': 'simple',
            'ts': 'close',
            'lookback': 56,
            'params': {'b': 0.02},
            'func': rules.support_resistance,
        },
        'filter_2':{
            'id': 'filter_2',
            'type': 'simple',
            'ts': 'close',
            'lookback': 7,
            'params': {'b': 0.015},
            'func': rules.support_resistance,
        },
        'filter_rule_extended': {
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
        }
    }
    config_r1 = {
        'rules': [single_rules['filter_1']],
        'strategy': {'type': 'fixed', 'strategy_rules': ['filter_1']}
    }
    config_r2 = {
        'rules': [single_rules['filter_2']],
        'strategy': {'type': 'fixed', 'strategy_rules': ['filter_2']}
    }
    config_r3 = {
        'rules': [
            single_rules['filter_1'],
            single_rules['filter_2'], 
            single_rules['filter_rule_extended']
        ],
        'strategy': {'type': 'fixed', 'strategy_rules': ['filter_rule_extended']}
    }
    rules_configs = zip(('r1', 'r2', 'r3'), (config_r1, config_r2, config_r3))

    # generate signals
    data_collector = gpw_data.GPWData()
    symbol = 'CCC'
    symbol_data = data_collector.load(symbols=symbol, from_csv=True, df=True)
    symbol_data = data_collector.detrend(symbol_data)
    rules_signals = {}
    for rc in rules_configs:
        sg = signal_generator.SignalGenerator(
            df = symbol_data,
            config = rc[1],
        )
        rules_signals[rc[0]] = sg.generate()
    print(rules_signals)

    # backtest data
    rules_results = {}
    position_sizer = position_size.FixedCapitalPerc(capital_perc=0.1)
    for rs_name, rs_signal in rules_signals.items():
        tester = backtester.Backtester(
            {symbol: rs_signal},
            position_sizer=position_sizer,
            price_label='adj_close'
        )
        tester_results, tester_trades = tester.run()
        tester_results.loc[:, 'daily_returns'] = results.get_daily_returns(tester_results)
        rules_results[rs_name] = tester_results
    print(rules_results)

    # create sampling distribution
    print('Creating sampling distribution')
    sampling_dist = wrc_sampling_dist(rules_results)
    print('This is part of sampling distr: ', sampling_dist[:10])

    avg_daily_returns = {r: df['daily_returns'].mean() for r, df in rules_results.items()}
    print('those are avg daily returns for all the tested rules: ', avg_daily_returns)

    highest_daily_ret = max(avg_daily_returns.values())
    for rule, ret in avg_daily_returns.items():
        # note: if multiple rules have same highest return, first one encountered will be chosen
        if ret == highest_daily_ret:
            best_rule = rule
    print(f'Best rule is "{rule}" with avg. daily ret equal to {highest_daily_ret}')

    # find p-val and asses statistical significancee
    exceeds_observed = [x for x in sampling_dist if x > highest_daily_ret]
    pval = len(exceeds_observed)/len(sampling_dist)
    print(f'p-val is {pval}')
    if pval <= 0.001:
        print(f'Result for the best rule ({best_rule}) is highly significant')
    elif pval <= 0.01:
        print(f'Result for the best rule ({best_rule}) is very significant')    
    elif pval <= 0.05:
        print(f'Result for the best rule ({best_rule}) is statistically significant')
    else:
        print(f'Best rule ({best_rule}) has no predictive power. It is highly possible that best rule average returns are 0')


if __name__ == '__main__':
    main()

