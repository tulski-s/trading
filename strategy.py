# built-in
import csv
import itertools
import traceback

# custom
import backtester
import commons
import results


def optimize_strategy(
        data=None, signal_gen_func=None, strategy_args=None, strategy_kwargs=None, 
        position_sizer=None, init_capital=None, optimize_for='sharpe', show_all=False,
        logger=None, debug=None, results_path=None
    ):
    log = commons.setup_logging(logger=logger, debug=debug)
    options = {}
    # pack args and kwargs together into one dict
    if strategy_args:
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
    if results_path:
        fh = open(results_path, 'w')
        writer = csv.writer(fh)
        writer.writerow([all_names])
    for combination in combinations:
        # unpack args and kwargs for each combination
        args = [a for a in combination[:len(args_names)]]
        kwargs = dict(zip(kwargs_names, combination[len(args_names):]))
        try:
            signals = {
                symbol_key: signal_gen_func(symbol_data, *args, **kwargs)
                for symbol_key, symbol_data in data.items() if not symbol_data.empty
            }
            backtest = backtester.Backtester(signals, position_sizer=position_sizer, init_capital=init_capital)
            res, trades = backtest.run()
            metrics = results.evaluate(res, trades)
            if results_path:
                writer.writerow([combination, metrics, metrics[optimize_for]])
        except Exception:
            log.warning('Not able to run optimization for: {combination}{names}. Got following exception:\n{err}'.format(
                combination=combination, names=all_names, err=traceback.format_exc()
            ))
            continue
        all_metrics.append((all_names, combination, metrics))
    all_metrics = sorted(all_metrics, key=lambda x: x[2][optimize_for], reverse=True)
    if results_path:
        fh.close()
    if show_all:
        return all_metrics
    return all_metrics[0]

