# thrid party
import pandas as pd

# custom
import position_size
import strategy

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
    return pd.DataFrame(price_and_entries, index=pd.DatetimeIndex(dates))


def test_optimize_strategy_correct_best():
    data = {'TEST_SIGS_1': pd.DataFrame({'close':[1,2,3]}, index=pd.DatetimeIndex(['2019-01-01', '2019-01-02', '2019-01-03'])),}
    strategy_args = [['long_only', 'long_and_short']]
    strategy_kwargs = {'arg2': [1, 2]}
    res = strategy.optimize_strategy(
        data=data,
        signal_gen_func=func_gen_test_signals,
        strategy_args=strategy_args,
        strategy_kwargs=strategy_kwargs,
        position_sizer=position_size.MaxFirstEncountered(),
        init_capital=10000,
    )
    expected_all_args = {'arg0': 'long_and_short', 'arg2': 1}
    best_all_args_from_optimization = dict(zip(res[0], res[1]))
    assert(best_all_args_from_optimization == expected_all_args)

