import numpy as np
import pandas as pd


def intraday(func):
    def wrapper(trading_df: pd.DataFrame, **kwargs):
        df = func(trading_df, **kwargs)

        df.loc[(df.index.time >= pd.to_datetime('14:30').time()) | (df.index.time < pd.to_datetime('09:30').time()), 'Position'] = 0

        return df
    
    return wrapper