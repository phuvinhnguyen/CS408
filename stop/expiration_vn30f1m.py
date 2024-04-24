import pandas as pd
from datetime import time
from ..data.vn30f1m_algotrade import get_third_thursday

THIRD_THUSDAY_SET = {get_third_thursday(year, month).date() for year in range(2018, 2025) for month in range(1, 13)}


def expiration(func):
    def wrapper(trading_df: pd.DataFrame, **kwargs):
        df = func(trading_df, **kwargs)

        df.loc[df.index.map(lambda x: x.date() in THIRD_THUSDAY_SET and x.time() >= time(14, 30)), 'Position'] = 0

        return df
    
    return wrapper