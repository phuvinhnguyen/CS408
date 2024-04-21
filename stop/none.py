import pandas as pd
import numpy as np


def tradingstop(func):
    def wrapper(trading_df: pd.DataFrame, **kwargs):
        trading_df.loc[:, 'Position'] = np.nan
        
        df: pd.DataFrame = func(trading_df, **kwargs)

        df.ffill(inplace=True)
        df.fillna(0, inplace=True)

        return df
    return wrapper