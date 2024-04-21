import pandas as pd
import numpy as np

def tradingstop(func):
    def wrapper(trading_df: pd.DataFrame, **kwargs):
        trading_df.loc[:, 'Position'] = np.nan

        df: pd.DataFrame = func(trading_df, **kwargs).copy(deep=True)

        alpha = kwargs.get('alpha', 0.8)
        beta = kwargs.get('beta', 0.8)

        max_profit = None
        buy_price = None
        position = None

        for index, row in df.iterrows():
            if pd.isna(df['Position'][index]) and position is not None:
                if position == 1:
                    max_profit = max(max_profit, row['Close'])

                    if row['Close'] < (max_profit - buy_price) * alpha + buy_price - beta:
                        position = None
                        max_profit = None
                        buy_price = None
                        df.at[index, 'Position'] = 0
                    else:
                        df.at[index, 'Position'] = 1
                elif position == -1:
                    max_profit = min(max_profit, row['Close'])

                    if row['Close'] > (max_profit - buy_price) * alpha + buy_price + beta:
                        position = None
                        max_profit = None
                        buy_price = None
                        df.at[index, 'Position'] = 0
                    else:
                        df.at[index, 'Position'] = -1
            else:
                position = df['Position'][index]
                max_profit = df['Close'][index]
                buy_price = df['Close'][index]

        df.fillna(0, inplace=True)

        return df
              
    return wrapper