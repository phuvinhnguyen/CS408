import pandas as pd
import pandas_ta as ta

def std(trading: pd.DataFrame, value='Close', name='', **kwargs):
    df = trading.copy(deep=True)

    period = kwargs.get(f'period{name}')
    df[f'std{name}'] = df[value].rolling(window=period).std()

    return df

def deviation2std(trading: pd.DataFrame, value='Close', name='', **kwargs):
    df = trading.copy(deep=True)

    diff_range = kwargs.get(f'diff_range{name}', 1)
    period = kwargs.get(f'period{name}', 5)
    df[f'dev2std{name}'] = df[value].diff(diff_range).diff(diff_range).rolling(window=period).std()

    return df

def sharpe(trading: pd.DataFrame, value='Close', name='', **kwargs):
    df = trading.copy(deep=True)

    period = kwargs.get(f'period{name}')
    df[f'sharpe{name}'] = df[value].rolling(period).apply(lambda x: ta.sharpe_ratio(x))

    return df