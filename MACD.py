import pandas as pd
import pandas_ta as ta

def macd(trading: pd.DataFrame, value='Close', name='', **kwargs):
    df = trading.copy(deep=True)

    fast = kwargs.get(f'fast{name}')
    slow = kwargs.get(f'slow{name}')
    signal = kwargs.get(f'signal{name}')
    diff_range = kwargs.get(f'diff_range{name}', 1)

    macd = ta.macd(df[value], fast=fast, slow=slow, signal=signal, append=True)

    name_mapper = {v: v.split('_')[0]+name for v in macd.columns}

    macd.rename(columns=name_mapper, inplace=True)

    df.loc[:, macd.columns] = macd

    df[f'MACDhv{name}'] = df[f'MACDh{name}'].diff(diff_range)
    df[f'MACDha{name}'] = df[f'MACDhv{name}'].diff(diff_range)

    df[f'MACDv{name}'] = df[f'MACD{name}'].diff(diff_range)
    df[f'MACDa{name}'] = df[f'MACDv{name}'].diff(diff_range)

    return df