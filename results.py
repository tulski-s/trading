def evaluate(results_df, trades):
    """
    Input: results dataframe from backtester run

    Outputs:
        - Daily Returns
        - Annualized Sharpe Ratio
        - Maximum Drowndown
        - Maximum Drowndown Duration
        - Annualized Returns
        - Expectation
        - No. of trades
    """
    df = results_df.copy()

    # Daily Returns - To find the return 𝑅(𝑡1,𝑡2) between dates 𝑡1 and 𝑡2 one takes 𝑅(𝑡1,𝑡2)=𝑁𝐴𝑉(𝑡2)/𝑁𝐴𝑉(𝑡1)−1
    df.loc[:, 'prev_day_nav'] = df['nav'].shift(1)
    df.loc[:, 'daily_returns'] = df['nav']/df['prev_day_nav']-1

    # Annualized Sharpe Ratio
    risk_free_rate = 0.02
    sessions_per_year = 252
    excess_returns = df['daily_returns'] - 0.02/252
    mean_er = excess_returns.mean()
    std_dev_er = excess_returns.std()
    annualized_sharpe_ratio=(252**.5)*mean_er/std_dev_er

    # Maximum Drowndown
    df.loc[:, 'highwatermark'] = df['rate_of_return'].cummax()
    df.loc[:, 'drawdown'] = df['highwatermark'] - df['rate_of_return']
    maximum_drawdown = df['drawdown'].max()

    # Maximum Drowndown Duration - cumulative sum of drowdown occurence which "restarts" everytime it encounters 0
    df.loc[:, 'is_dd'] = df['drawdown'] != 0
    df.loc[:, 'drowdown_duration'] = \
        df['is_dd'].cumsum()-df['is_dd'].cumsum().where(~df['is_dd']).ffill().fillna(0).astype(int)
    maximum_drawdown_duration = df.loc[:, 'drowdown_duration'].max()

    # Annualized Return - ((1 + cumulative_return%) ** (365/days held))-1
    annualized_return = (((1+(df['rate_of_return'].iloc[-1]/100))**(365/df.shape[0]))-1)*100

    # Expectation - Win% * Avg_Win_Size - Loss% * Avg_Loss_Size
    profits = []
    losses = []
    for trade in trades.values():
        # trade was not closed after backtest
        if ('sell_value_with_fee' not in trade.keys()):
            continue
        if profit > 0:
            profits.append(profit)
        else:
            losses.append(profit)

    no_trades = len(trades)
    avg_win = sum(profits)/len(profits)
    avg_loss = abs(sum(losses)/len(losses))
    expectation = ((len(profits)/no_trades)*avg_win) - ((len(losses)/no_trades)*avg_loss)
    return {
        'sharpe': annualized_sharpe_ratio,
        'max_dd': maximum_drawdown,
        'max_dd_duration': maximum_drawdown_duration,
        'annualized_return': annualized_return,
        'no_trades': no_trades,
        'expectation': expectation,
    }
    


def main():
    pass



if __name__ == '__main__':
    main()