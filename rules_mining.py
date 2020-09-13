# built-in
import math
import random
import time

# 3rd party
import numpy as np

# custom
import backtester
import gpw_data
import position_size
import results
import rules
import signal_generator


def same_lengths_assertion(lengths):
    try:
        assert(len(set(lengths)) == 1)
    except AssertionError:
        raise AssertionError(
            """
            Not all rules have the same number of results. For method to work properly, each rule should have exactly 
            the same dates (number of daily returns). Results legnts: {}
            """.format(lengths)
        )


def pval_msg(pval):
    if pval <= 0.001:
        print('Result is highly significant')
    elif pval <= 0.01:
        print('Result is very significant')    
    elif pval <= 0.05:
        print(f'Result is statistically significant')
    else:
        print(f'Rule has no predictive power')


def create_wrc_sampling_dist(rules_results, daily_ret_col='daily_returns', no_samples=5000, batch=64):
    """
    Input: dict with rule_names and DataFrame with daily returns column present
    """
    # perepare data
    lengths = []
    results = []
    for df in rules_results.values():
        rets = df[daily_ret_col].tolist()
        results.append(rets)
        lengths.append(len(rets))
    same_lengths_assertion(lengths)
    results = np.array(results).T
    results = np.nan_to_num(results, nan=0)
    # center returns
    avgs = results.mean(axis=0)
    results = results - avgs
    # get wrc sampling dist
    sample_size = lengths[0]
    sample_idxs = np.array(range(sample_size))
    max_avg_rets = []
    for k in range(no_samples):
        # random sampling with replacement
        random_idxs = np.random.choice(sample_idxs, size=sample_size, replace=True)
        max_avg = max(results[random_idxs,i:i+batch].mean(axis=0).max() for i in range(0, results.shape[1], batch))
        max_avg_rets.append(max_avg)
    return max_avg_rets


def create_mc_sampling_distr(rules_states, price_changes, no_samples=5000):
    """
    Creates sampling distribution for the Monte Carlo method. This sampling distribution represent expected return of
    useless (random) rule. In high level it is done via random assignment of rule's values to market returns.
    MC's NULL hypothesis is simply that all rules tested have output values that are randomly correlated with future
    marrket behaviour.

    Input: 
        `rules_states` is np.array with rule states (-1,0,1)
        `price_changes` is np.array with percentage change of actual prices

    Method:
    - obtian daily rule output states (-1,1 or -1,0,1)
    - rule outputs are paired with random day market returns. pairing should be consist across all the rules. that is,
      if (o1, m15), that is output from day 1 is paired with market return from day 15, this pairing should be same for
      all rules
    - determine mean rate of return for each rule (avg daily returns)
    - select highest mean return as entry for sampling distribution
    """
    lengths = [len(positions) for positions in rules_states.values()]
    same_lengths_assertion(lengths)
    assert(lengths[0] == len(price_changes))

    max_avg_rets = []
    for k in range(no_samples):
        changes = np.random.choice(price_changes, size=len(price_changes), replace=False)
        avg_rets = [(states*changes).mean() for states in rules_states.values()]
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
    print('generating signals')
    data_collector = gpw_data.GPWData()
    symbol = 'CCC'
    symbol_data = data_collector.load(symbols=symbol, from_csv=True, df=True)
    symbol_data = data_collector.detrend(symbol_data)
    rules_signals = {}
    rules_states = {}
    for rc in rules_configs:
        sg = signal_generator.SignalGenerator(
            df = symbol_data,
            config = rc[1],
        )
        rules_signals[rc[0]] = sg.generate()
        rules_states[rc[0]] = np.array(sg.final_positions)

    # backtest data
    print('backtesting data')
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
        tester_results.loc[:, 'actual_price_change'] = results.get_price_change(rs_signal)
        rules_results[rs_name] = tester_results

    avg_daily_returns = {r: df['daily_returns'].mean() for r, df in rules_results.items()}
    print('those are avg daily returns for all the tested rules: ', avg_daily_returns)
    highest_daily_ret = max(avg_daily_returns.values())
    for rule, ret in avg_daily_returns.items():
        # note: if multiple rules have same highest return, first one encountered will be chosen
        if ret == highest_daily_ret:
            best_rule = rule
    print(f'Best rule is "{rule}" with avg. daily ret equal to {highest_daily_ret}')

    # White's Reality Check
    print("     #### White's Reality Check")
    # create sampling distribution (small amount of samples just to make it faster)
    t1 = time.time()
    wrc_sampling_dist = create_wrc_sampling_dist(rules_results, no_samples=10000)
    t2 = time.time()
    print(f'Whites Reality Check took: {round(t2-t1, 2)} seconds')
    # find p-val and asses statistical significancee
    wrc_exceeds_observed = [x for x in wrc_sampling_dist if x >= highest_daily_ret]
    wrc_pval = len(wrc_exceeds_observed)/len(wrc_sampling_dist)
    print(f'p-val for WRC is {wrc_pval}')
    pval_msg(wrc_pval)

    # Monte Carlo simulation
    print("     #### Monte Carlo simulation")
    # create sampling distr for Monte Carlo. again just small sample size to make it faster
    price_changes = rules_results[list(rules_results.keys())[0]]['actual_price_change'].to_numpy()
    t3 = time.time()
    mc_sampling_dist = create_mc_sampling_distr(rules_states, price_changes, no_samples=10000)
    t4 = time.time()
    print(f'Monte Carlo took: {round(t4-t3, 2)} seconds')
    mc_exceeds_observed = [x for x in mc_sampling_dist if x >= highest_daily_ret]
    mc_pval = len(mc_exceeds_observed)/len(mc_sampling_dist)
    print(f'p-val for MC is {mc_pval}')
    pval_msg(mc_pval)


if __name__ == '__main__':
    main()
