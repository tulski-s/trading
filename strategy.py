# buil in
import itertools
import traceback

# thrid party
import pandas as pd

# custom
import backtester
import commons
import position_size
import results


def optimize_strategy(
        data=None, signal_gen_func=None, strategy_args=None, strategy_kwargs=None, 
        position_sizer=None, init_capital=None, optimize_for='sharpe', show_all=False,
        logger=None, debug=None,
    ):
    log = commons.setup_logging(logger=logger, debug=debug)
    options = {}
    # pack args and kwargs together into one dict
    for idx, arg in enumerate(strategy_args):
        options['arg{}'.format(idx)] = arg
    options.update(strategy_kwargs)
    # create all combinations of agrs and kwargs
    args_names = ['arg{}'.format(idx) for idx in range(len(strategy_args or []))]
    kwargs_names = list(strategy_kwargs.keys())
    all_names = args_names+kwargs_names
    combinations = itertools.product(*(options[k] for k in all_names))
    # execute strategy for each comibinations
    all_metrics = []
    for combination in combinations:
        # unpack args and kwargs for each combination
        args = [a for a in combination[:len(args_names)]]
        kwargs = dict(zip(kwargs_names, combination[len(args_names):]))
        try:
            signals = signal_gen_func(data, *args, **kwargs)
            backtest = backtester.Backtester(signals, position_sizer=position_sizer, init_capital=init_capital)
            res, trades = backtest.run()
            metrics = results.evaluate(res, trades)
        except Exception:
            log.warning('Not able to run optimization for: {combination}{names}. Got following exception:\n{err}'.format(
                combination=combination, names=all_names, err=traceback.format_exc()
            ))
            continue
        # TODO(stulski): here also should be optional saving into the itermidiate file in case of long runs
        all_metrics.append((all_names, combination, metrics))
    all_metrics = sorted(all_metrics, key=lambda x: x[2][optimize_for], reverse=True)
    if show_all:
        return all_metrics
    return all_metrics[0]


def func_gen_test_signals(df, arg1, arg2=None):
    """
    `arg` tests positional arguments
    `arg2` tests kwargs
    """
    dates = [
        '2019-01-01', '2019-01-02', '2019-01-03', '2019-01-04', '2019-01-05', '2019-01-06', 
        '2019-01-07', '2019-01-08', '2019-01-09', '2019-01-10', '2019-01-11', '2019-01-12', 
        '2019-01-13', '2019-01-14', '2019-01-15'
    ]
    price_and_entries = {
        'close': [120,140,135,120,110,156,145,178,198,191,188,184,180,175,174],
    }
    if arg1 == 'long_only':
        if arg2 == 1:
            entries = {
                'entry_long': [0,0,1,0,0,0,0,1,0,0,0,1,0,0,0],
                'exit_long': [0,0,0,0,0,1,0,0,0,1,0,0,0,0,1],
                'entry_short': [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                'exit_short': [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
            }
        elif arg2 == 2:
            # will throw an error
            entries = {}
    elif arg1 == 'long_and_short':
        if arg2 == 1:
            entries = {
                'entry_long': [1,0,0,0,0,1,0,0,1,0,0,0,0,1,0],
                'exit_long': [0,0,1,0,0,0,1,0,0,1,0,0,0,0,1],
                'entry_short': [0,0,0,1,0,0,0,0,0,0,1,0,0,0,0],
                'exit_short': [0,0,0,0,1,0,0,0,0,0,0,0,1,0,0],
            }
        elif arg2 == 2:
            entries = {
                'entry_long': [0,0,0,1,0,0,0,0,1,0,0,0,1,0,0],
                'exit_long': [0,0,0,0,0,1,0,0,0,1,0,0,0,0,1],
                'entry_short': [1,0,0,0,0,0,1,0,0,0,1,0,0,0,0],
                'exit_short': [0,0,1,0,0,0,0,1,0,0,0,1,0,0,0],
            }
    price_and_entries.update(entries)
    return {
        'TEST_SIGS_1': pd.DataFrame(price_and_entries, index=pd.DatetimeIndex(dates)),
    }


def main():
    data = {'mock_symbol': 'mock_data'}
    strategy_args = [['long_only', 'long_and_short']]
    strategy_kwargs = {'arg2': [1, 2]}
    res = optimize_strategy(
        data=data,
        signal_gen_func=func_gen_test_signals,
        strategy_args=strategy_args,
        strategy_kwargs=strategy_kwargs,
        position_sizer=position_size.MaxFirstEncountered(),
        init_capital=10000,
        show_all=True,
    )
    print('Optimization results: ', res)


if __name__ == '__main__':
    main()

