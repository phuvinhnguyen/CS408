import pandas as pd
from ..tools.plot_entry import plot_entry_points
import pandas_ta as ta
import matplotlib.pyplot as plt

def plot_negative_gain(backtest_output, trading_df, date_range=3, plot_number = 10, ploting_params = ['Close']):
    trading = trading_df.copy(deep=True)
    df_check = backtest_output.df_brief
    df_check['start_date'] = df_check.index
    df_check['start_date'] = df_check['start_date'].shift(1)
    df_check = df_check[df_check['gain'] < 0]
    df_check = df_check.sort_values(by='gain')
    
    trading['start_datetime'] = trading.index
    trading['end_datetime'] = trading.index

    trading['start_datetime'] = trading['start_datetime'].shift(date_range)

    row = df_check.iloc[plot_number]
    start_date = row['start_date']
    end_date = row.name
    
    negative_df = trading[(trading.index >= start_date) & (trading.index <= end_date)]

    start_date = negative_df['start_datetime'].iloc[0]
    end_date = negative_df['end_datetime'].iloc[-1]

    negative_df = trading[(trading.index >= start_date) & (trading.index <= end_date)]

    plot_entry_points(negative_df, reset_index=False, ploting_params=ploting_params)

def analysis(trading_df, fee=0.2, init_asset=1000, visual=True):
    trading = trading_df.copy(deep=True)
    trading['start_point'] = trading['Position'].diff()
    trading = trading[trading['start_point'] != 0][1:]
    trading['last_trade_profit'] = trading['Position'] * (trading['Close'].shift(-1) - trading['Close'])
    trading['last_trade_profit_with_fee'] = trading['Position'] * (trading['Close'].shift(-1) - trading['Close']) - fee * trading['Position'].abs()

    trading['cummulated_profit'] = trading['last_trade_profit'].cumsum() + init_asset
    trading['cummulated_return'] = trading['cummulated_profit'] / init_asset

    trading['cummulated_profit_with_fee'] = trading['last_trade_profit_with_fee'].cumsum() + init_asset
    trading['cummulated_return_with_fee'] = trading['cummulated_profit_with_fee'] / init_asset

    trading.dropna(inplace=True)

    mdd = ta.max_drawdown(trading['cummulated_profit'])
    sharpe = ta.sharpe_ratio(trading['cummulated_profit'])
    sharpe_after_fee = ta.sharpe_ratio(trading['cummulated_profit_with_fee'])
    number_of_trade = trading[trading['Position'] != 0]['Position'].count()

    if visual:
        # Print value
        print(f'mdd: {mdd}')
        print(f'sharpe: {sharpe}')
        print(f'sharpe_after_fee: {sharpe_after_fee}')
        print(f'number_of_trade: {number_of_trade}')

        # Plot profit
        plt.figure(figsize=(17, 7))
        plt.grid(True)
        plt.plot(trading['cummulated_profit'], label='cummulated_profit')
        plt.plot(trading['cummulated_profit_with_fee'], label='cummulated_profit_with_fee')
        plt.legend()
        plt.show()

    return {
        'dataframe': trading,
        'mdd': mdd,
        'sharpe': sharpe,
        'sharpe_after_fee': sharpe_after_fee,
        'number_of_trade': number_of_trade,
    }
