

# custom
from backtester import run_test_strategy

def evaluate(results_df, trades):
    """
    Input: results dataframe from backtester run

    Output:
        OK - Daily Returns
        OK - Annualized Sharpe Ratio
        OK - Maximum Drowndown
        OK - Maximum Drowndown Duration
        OK - Annualized Returns
        TODO - Expectation
        TODO - No. of trades
    """
    df = results_df.copy()

    # Daily Returns - To find the return 𝑅(𝑡1,𝑡2) between dates 𝑡1 and 𝑡2 one takes 𝑅(𝑡1,𝑡2)=𝑁𝐴𝑉(𝑡2)/𝑁𝐴𝑉(𝑡1)−1
    df.loc[:, 'prev_day_nav'] = df['nav'].shift(1)
    df.loc[:, 'daily_returns'] = df['nav']/df['prev_day_nav']-1
    # print(excess_returns)

    # Annualized Sharpe Ratio
    risk_free_rate = 0.02
    sessions_per_year = 252
    excess_returns = df['daily_returns'] - 0.02/252
    mean_er = excess_returns.mean()
    std_dev_er = excess_returns.std()
    annualized_sharpe_ratio=(252**.5)*mean_er/std_dev_er
    # print('Sharpe: ', annualized_sharpe_ratio)

    # Maximum Drowndown
    df.loc[:, 'highwatermark'] = df['rate_of_return'].cummax()
    df.loc[:, 'drawdown'] = df['highwatermark'] - df['rate_of_return']
    maximum_drawdown = df['drawdown'].max()
    # print('maximum drawdown is: ', maximum_drawdown)

    # Maximum Drowndown Duration - cumulative sum of drowdown occurence which "restarts" everytime it encounters 0
    df.loc[:, 'is_dd'] = df['drawdown'] != 0
    df.loc[:, 'drowdown_duration'] = \
        df['is_dd'].cumsum()-df['is_dd'].cumsum().where(~df['is_dd']).ffill().fillna(0).astype(int)
    maximum_drawdown_duration = df.loc[:, 'drowdown_duration'].max()
    # print('maximum drawdown duration is: ', maximum_drawdown_duration)

    # Annualized Return - ((1 + cumulative_return%) ** (365/days held))-1
    annualized_return = (((1+(df['rate_of_return'].iloc[-1]/100))**(365/df.shape[0]))-1)*100
    # print('annualized return: ', annualized_return)

    # Expectation - Win% * Avg_Win_Size - Loss% * Avg_Loss_Size
    print(trades[list(trades.keys())[0]])

    print(trades)
    # win_rate
    # loose_rate
    for trade in trades:
        pass

    # print('\n')
    # print(df.head(5))

    """
    short trade example
    {
        'buy_ds': Timestamp('2014-09-03 00:00:00'),
        'type': 'short', 'trx_value_no_fee': 1938.2999999999997,
        'trx_value_gross': 1930.9299999999998,
        'sell_ds': Timestamp('2014-09-04 00:00:00'),
        'sell_value_no_fee': -1960.0,
        'sell_value_gross': -1964.0}
    }

    cos za male "fee"..... sprawdz to!

    """

    


def main():
    results, trades = run_test_strategy()

    print(results.head(5))

    evaluate(results, trades)


if __name__ == '__main__':
    main()