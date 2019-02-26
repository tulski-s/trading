# 3rd party
import pandas as pd
import pytest


dates = ['2010-09-28', '2010-09-29', '2010-09-30', '2010-10-01']
signals = {
    'close': [100, 210, 90, 80],
    'entry_long': [1,0,0,0],
    'exit_long': [0,1,0,0],
    'entry_short': [0,0,1,0],
    'exit_short': [0,0,0,1],
}
signals_df = pd.DataFrame(signals, index=pd.DatetimeIndex(dates))

print(signals_df)


