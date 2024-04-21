import pandas as pd
import numpy as np

def tradingstop(func):
    def wrapper(trading_df: pd.DataFrame, **kwargs):
        trading_df.loc[:, 'Position'] = np.nan

        df: pd.DataFrame = func(trading_df, **kwargs).copy(deep=True)

        alpha = kwargs.get('alpha', 0.8)

        max_profit = None
        position = None

        for index, row in df.iterrows():
            if position == 1 or df['Position'][index] == 1:
                if max_profit is not None:
                    max_profit = max(max_profit, row['Close'])
                else:
                    max_profit = row['Close']
                position = 1

                if row['Close'] < max_profit * alpha:
                    position = None
                    max_profit = None
                    df.at[index, 'Position'] = 0
                else:
                    df.at[index, 'Position'] = 1
            elif position == -1 or df['Position'][index] == -1:
                if max_profit is not None:
                    max_profit = min(max_profit, row['Close'])
                else:
                    max_profit = row['Close']
                position = -1

                if row['Close'] > max_profit * (2 - alpha):
                    position = None
                    max_profit = None
                    df.at[index, 'Position'] = 0
                else:
                    df.at[index, 'Position'] = -1

        df.ffill(inplace=True)
        df.fillna(0, inplace=True)

        return df
              

    return wrapper